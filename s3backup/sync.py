# -*- coding: utf-8 -*-

import logging

import tqdm

from s3backup.clients import SyncState


logger = logging.getLogger(__name__)


class DeferredFunction(object):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        self.func(*self.args, **self.kwargs)

    def __eq__(self, other):
        if not isinstance(other, DeferredFunction):
            return False
        return self.func == other.func and self.args == other.args and self.kwargs == other.kwargs

    def __repr__(self):
        return 'DeferredFunction<func={func}, args={args}, kwargs={kwargs}>'.format(
            func=self.func,
            args=self.args,
            kwargs=self.kwargs,
        )


def sync(client_1, client_2):
    try:
        deferred_calls, unhandled_events = get_sync_actions(client_1, client_2)

        logger.debug('There are %s unhandled events for the user to solve', len(unhandled_events))
        logger.debug('There are %s automatically deferred calls', len(deferred_calls))
        if len(unhandled_events) > 0:
            logger.debug('%s', unhandled_events)
            for key, (action_1, action_2) in unhandled_events.items():
                logger.info(
                    '\nConflict for "%s". Which version would you like to keep?\n'
                    '   (1) %s%s %s at %s\n'
                    '   (2) %s%s %s at %s\n'
                    '   (3) Skip this file',
                    key,
                    client_1.get_uri(), key, action_1.action, action_1.get_datetime(),
                    client_2.get_uri(), key, action_2.action, action_2.get_datetime(),
                )
                choice = input('Choice (default=skip): ')
                logger.info('')
                if choice == '1':
                    deferred_calls[key] = get_deferred_function(key, action_1, client_2, client_1)
                elif choice == '2':
                    deferred_calls[key] = get_deferred_function(key, action_2, client_1, client_2)
                else:
                    continue
    except KeyboardInterrupt:
        logger.warning('Session interrupted by Keyboard Interrupt. Aborting....')
        return

    # call everything once we know we can handle all of it
    logger.debug('There are %s total deferred calls', len(deferred_calls))
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


def get_sync_actions(client_1, client_2):
    # we store a list of deferred calls to make sure we can handle everything before
    # running any updates on the file system and indexes
    deferred_calls = {}

    # list of unhandled events which cannot be solved automatically (or alternatively the
    # the automated solution has not yet been implemented)
    unhandled_events = {}

    logger.debug('Generating deferred calls based on client states')
    for key, action_1, action_2 in get_actions(client_1, client_2):
        logger.debug('%s: %s %s', key, action_1, action_2)
        if action_1.action == SyncState.NOCHANGES and action_2.action == SyncState.NOCHANGES:
            if action_1.remote_timestamp == action_2.remote_timestamp:
                continue
            elif action_1.remote_timestamp > action_2.remote_timestamp:
                deferred_calls[key] = DeferredFunction(
                    update_client, client_2, client_1, key, action_1.remote_timestamp
                )
            elif action_2.remote_timestamp > action_1.remote_timestamp:
                deferred_calls[key] = DeferredFunction(
                    update_client, client_1, client_2, key, action_2.remote_timestamp
                )

        elif action_1.action == SyncState.CREATED and action_2.action == SyncState.DOESNOTEXIST:
            deferred_calls[key] = DeferredFunction(
                create_client, client_2, client_1, key, action_1.local_timestamp
            )

        elif action_2.action == SyncState.CREATED and action_1.action == SyncState.DOESNOTEXIST:
            deferred_calls[key] = DeferredFunction(
                create_client, client_1, client_2, key, action_2.local_timestamp
            )

        elif action_1.action == SyncState.NOCHANGES and action_2.action == SyncState.DOESNOTEXIST:
            deferred_calls[key] = DeferredFunction(
                update_client, client_2, client_1, key, action_1.remote_timestamp
            )

        elif action_2.action == SyncState.NOCHANGES and action_1.action == SyncState.DOESNOTEXIST:
            deferred_calls[key] = DeferredFunction(
                update_client, client_1, client_2, key, action_2.remote_timestamp
            )

        elif (
            action_1.action == SyncState.UPDATED and
            action_2.action == SyncState.NOCHANGES and
            action_1.remote_timestamp == action_2.remote_timestamp
        ):
            deferred_calls[key] = DeferredFunction(
                update_client, client_2, client_1, key, action_1.local_timestamp
            )

        elif (
            action_2.action == SyncState.UPDATED and
            action_1.action == SyncState.NOCHANGES and
            action_1.remote_timestamp == action_2.remote_timestamp
        ):
            deferred_calls[key] = DeferredFunction(
                update_client, client_1, client_2, key, action_2.local_timestamp
            )

        elif action_1.action == SyncState.UPDATED and action_2.action == SyncState.DOESNOTEXIST:
            deferred_calls[key] = DeferredFunction(
                update_client, client_2, client_1, key, action_1.local_timestamp
            )

        elif action_2.action == SyncState.UPDATED and action_1.action == SyncState.DOESNOTEXIST:
            deferred_calls[key] = DeferredFunction(
                update_client, client_2, client_1, key, action_1.local_timestamp
            )

        elif (
            action_1.action == SyncState.DELETED and
            action_2.action == SyncState.NOCHANGES and
            action_1.remote_timestamp == action_2.remote_timestamp
        ):
            deferred_calls[key] = DeferredFunction(
                delete_client, client_2, key, action_1.local_timestamp
            )

        elif (
            action_2.action == SyncState.DELETED and
            action_1.action == SyncState.NOCHANGES and
            action_1.remote_timestamp == action_2.remote_timestamp
        ):
            deferred_calls[key] = DeferredFunction(
                delete_client, client_1, key, action_2.local_timestamp
            )

        elif (
            action_1.action in (SyncState.DELETED, SyncState.DOESNOTEXIST) and
            action_2.action in (SyncState.DELETED, SyncState.DOESNOTEXIST)
        ):
            # nothing to do, they have already both been deleted/do not exist
            continue

        # TODO: Check DELETE timestamp. if it is older than you should be able to safely ignore it

        else:
            unhandled_events[key] = (action_1, action_2)

    return deferred_calls, unhandled_events


def get_actions(client_1, client_2):
    keys_1 = client_1.get_all_keys()
    keys_2 = client_2.get_all_keys()
    all_keys = set(keys_1) | set(keys_2)
    logger.debug(
        '%s keys in total (%s for %s and %s for %s)',
        len(all_keys), len(keys_1), client_1.get_uri(), len(keys_2), client_2.get_uri()
    )
    client_1_actions = client_1.get_actions(all_keys)
    client_2_actions = client_2.get_actions(all_keys)

    for key in sorted(all_keys):
        yield key, client_1_actions[key], client_2_actions[key]


def get_deferred_function(key, action, to_client, from_client):
    if action.action == 'UPDATED':
        return DeferredFunction(
            update_client, to_client, from_client, key, action.local_timestamp
        )
    elif action.action == 'CREATED':
        return DeferredFunction(
            create_client, to_client, from_client, key, action.local_timestamp
        )
    elif action.action == 'DELETED':
        return DeferredFunction(delete_client, to_client, key, action.remote_timestamp)
    else:
        raise ValueError('Unknown how to handle Action', action)


def get_progress_bar(max_value, desc):
    return tqdm.tqdm(
        total=max_value,
        leave=False,
        desc=desc,
        unit='B',
        unit_scale=True,
        mininterval=0.2,
    )


def create_client(to_client, from_client, key, timestamp):
    logger.info('Creating %s (%s => %s)', key, from_client.get_uri(), to_client.get_uri())
    move(to_client, from_client, key, timestamp)


def update_client(to_client, from_client, key, timestamp):
    logger.info('Updating %s (%s => %s)', key, from_client.get_uri(), to_client.get_uri())
    move(to_client, from_client, key, timestamp)


def move(to_client, from_client, key, timestamp):
    sync_object = from_client.get(key)

    with get_progress_bar(sync_object.total_size, key) as progress_bar:
        to_client.put(key, sync_object, callback=progress_bar.update)

    to_client.set_remote_timestamp(key, timestamp)
    from_client.set_remote_timestamp(key, timestamp)


def delete_client(client, key, remote_timestamp):
    logger.info('Deleting %s on %s', key, client.get_uri())
    client.delete(key)
    client.set_remote_timestamp(key, remote_timestamp)
