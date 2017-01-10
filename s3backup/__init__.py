# -*- coding: utf-8 -*-

import logging

import tqdm

from s3backup.clients import SyncState


logger = logging.getLogger('s3backup')


class DeferredFunction(object):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        self.func(*self.args, **self.kwargs)

    def __repr__(self):
        return 'DeferredFunction<func{func}, args={args}, kwargs={kwargs}>'.format(
            func=self.func,
            args=self.args,
            kwargs=self.kwargs,
        )


def get_progress_bar(max_value, desc):
    return tqdm.tqdm(
        total=max_value,
        leave=False,
        desc=desc,
        unit='B',
        unit_scale=True,
        mininterval=0.2,
    )


def update_client(to_client, from_client, key, timestamp):
    logger.info('Updating %s (%s => %s)', key, from_client.get_uri(), to_client.get_uri())
    sync_object = from_client.get(key)

    with get_progress_bar(sync_object.total_size, key) as progress_bar:
        to_client.put(key, sync_object, callback=progress_bar.update)

    to_client.set_remote_timestamp(key, timestamp)
    from_client.set_remote_timestamp(key, timestamp)


def delete_client(client, key, remote_timestamp):
    logger.info('Deleting %s on %s', key, client)
    client.delete(key)
    client.set_remote_timestamp(key, remote_timestamp)


def get_actions(client_1, client_2):
    keys_1 = client_1.get_all_keys()
    keys_2 = client_2.get_all_keys()

    all_keys = set(keys_1) | set(keys_2)
    for key in all_keys:
        action_1 = client_1.get_action(key)
        action_2 = client_2.get_action(key)
        yield key, action_1, action_2


def sync(client_1, client_2):
    # we store a list of deferred calls to make sure we can handle everything before
    # running any updates on the file system and indexes
    deferred_calls = {}

    for key, action_1, action_2 in get_actions(client_1, client_2):
        logger.debug('%s: %s %s', key, action_1, action_2)
        if action_1.action == SyncState.NONE and action_2.action == SyncState.NONE:
            if action_1.timestamp == action_2.timestamp:
                continue
            elif action_1.timestamp is None and action_2.timestamp:
                deferred_calls[key] = DeferredFunction(
                    update_client, client_1, client_2, key, action_2.timestamp
                )
            elif action_2.timestamp is None and action_1.timestamp:
                deferred_calls[key] = DeferredFunction(
                    update_client, client_2, client_1, key, action_1.timestamp
                )
            elif action_1.timestamp > action_2.timestamp:
                deferred_calls[key] = DeferredFunction(
                    update_client, client_2, client_1, key, action_1.timestamp
                )
            elif action_2.timestamp > action_1.timestamp:
                deferred_calls[key] = DeferredFunction(
                    update_client, client_1, client_2, key, action_2.timestamp
                )

        elif action_1.action == SyncState.UPDATED and action_2.action == SyncState.NONE:
            deferred_calls[key] = DeferredFunction(
                update_client, client_2, client_1, key, action_1.timestamp
            )

        elif action_2.action == SyncState.UPDATED and action_1.action == SyncState.NONE:
            deferred_calls[key] = DeferredFunction(
                update_client, client_1, client_2, key, action_2.timestamp
            )

        elif action_1.action == SyncState.DELETED and action_2.action == SyncState.NONE:
            deferred_calls[key] = DeferredFunction(delete_client, client_2, key, action_1.timestamp)

        elif action_2.action == SyncState.DELETED and action_1.action == SyncState.NONE:
            deferred_calls[key] = DeferredFunction(delete_client, client_1, key, action_2.timestamp)

        elif action_1.action == SyncState.DELETED and action_2.action == SyncState.DELETED:
            # nothing to do, they have already both been deleted
            continue

        # TODO: Check DELETE timestamp. if it is older than you should be able to safely ignore it

        else:
            raise ValueError(
                'Unhandled state, aborting before anything is updated',
                key, client_1, action_1, client_2, action_2
            )

    # call everything once we know we can handle all of it
    logger.debug('Deferred calls: %s', deferred_calls)
    try:
        for key, deferred_function in deferred_calls.items():
            try:
                deferred_function()
                client_1.update_index_entry(key)
                client_2.update_index_entry(key)
            except Exception as e:
                logger.error('An error occurred while trying to update %s: %s', key, e)
    except KeyboardInterrupt:
        logger.warning('Session interrupted by Keyboard Interrupt. Cleaning up....')

    if len(deferred_calls) > 0:
        logger.info('Flushing Index to Storage')
        client_1.flush_index()
        client_2.flush_index()
    else:
        logger.info('Nothing to update')
