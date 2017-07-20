# -*- coding: utf-8 -*-

import logging
import os
import subprocess
import tempfile

from clint.textui import colored

import tqdm

from s4.clients import SyncState


class DeferredFunction(object):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return self.func(*self.args, **self.kwargs)

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


def show_diff(client_1, client_2, key):
    so1 = client_1.get(key)
    data1 = so1.fp.read()
    so1.fp.close()

    so2 = client_2.get(key)
    data2 = so2.fp.read()
    so2.fp.close()

    fd1, path1 = tempfile.mkstemp()
    fd2, path2 = tempfile.mkstemp()
    fd3, path3 = tempfile.mkstemp()

    with open(path1, 'wb') as fp:
        fp.write(data1)
    with open(path2, 'wb') as fp:
        fp.write(data2)

    # This is a lot faster than the difflib found in python
    with open(path3, 'wb') as fp:
        subprocess.call([
            'diff', '-u',
            '--label', client_1.get_uri(key), path1,
            '--label', client_2.get_uri(key), path2,
        ], stdout=fp)

    subprocess.call(['less', path3])

    os.close(fd1)
    os.close(fd2)
    os.close(fd3)

    os.remove(path1)
    os.remove(path2)
    os.remove(path3)


class SyncWorker(object):
    def __init__(self, client_1, client_2):
        self.client_1 = client_1
        self.client_2 = client_2
        self.logger = logging.getLogger(str(self))

    def sync(self, conflict_choice=None):
        try:
            deferred_calls, unhandled_events = self.get_sync_states()

            self.logger.debug(
                'There are %s unhandled events for the user to solve', len(unhandled_events)
            )
            self.logger.debug(
                'There are %s automatically deferred calls', len(deferred_calls)
            )
            if len(unhandled_events) > 0:
                self.logger.debug('%s', unhandled_events)

                for key in sorted(unhandled_events.keys()):
                    action_1, action_2 = unhandled_events[key]
                    if conflict_choice is None:
                        self.logger.info(
                            '\nConflict for "%s". Which version would you like to keep?\n'
                            '   (1) %s%s updated at %s (%s)\n'
                            '   (2) %s%s updated at %s (%s)\n'
                            '   (d) View difference (requires the diff command)\n'
                            '   (X) Skip this file\n',
                            key,
                            self.client_1.get_uri(),
                            key, action_1.get_remote_datetime(), action_1.state,
                            self.client_2.get_uri(),
                            key, action_2.get_remote_datetime(), action_2.state,
                        )
                        while True:
                            choice = input('Choice (default=skip): ')
                            self.logger.info('')

                            if choice == 'd':
                                show_diff(self.client_1, self.client_2, key)
                            else:
                                break
                    else:
                        choice = conflict_choice

                    if choice == '1':
                        deferred_calls[key] = self.get_deferred_function(
                            key, action_1, self.client_2, self.client_1
                        )
                    elif choice == '2':
                        deferred_calls[key] = self.get_deferred_function(
                            key, action_2, self.client_1, self.client_2
                        )
                    else:
                        self.logger.info('Ignoring sync conflict for %s', key)
                        continue

        except KeyboardInterrupt:
            self.logger.warning('Session interrupted by Keyboard Interrupt. Aborting....')
            return

        self.run_deferred_calls(deferred_calls)

    def get_sync_states(self):
        # we store a list of deferred calls to make sure we can handle everything before
        # running any updates on the file system and indexes
        deferred_calls = {}

        # list of unhandled events which cannot be solved automatically (or alternatively the
        # the automated solution has not yet been implemented)
        unhandled_events = {}

        self.logger.debug('Generating deferred calls based on client states')
        for key, state_1, state_2 in self.get_states():
            self.logger.debug('%s: %s %s', key, state_1, state_2)
            if state_1.state == SyncState.NOCHANGES and state_2.state == SyncState.NOCHANGES:
                if state_1.remote_timestamp == state_2.remote_timestamp:
                    continue
                elif state_1.remote_timestamp > state_2.remote_timestamp:
                    deferred_calls[key] = DeferredFunction(
                        self.update_client, self.client_2, self.client_1,
                        key, state_1.remote_timestamp
                    )
                elif state_2.remote_timestamp > state_1.remote_timestamp:
                    deferred_calls[key] = DeferredFunction(
                        self.update_client, self.client_1, self.client_2,
                        key, state_2.remote_timestamp
                    )

            elif state_1.state == SyncState.CREATED and state_2.state == SyncState.DOESNOTEXIST:
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_2, self.client_1, key, state_1.local_timestamp
                )

            elif state_2.state == SyncState.CREATED and state_1.state == SyncState.DOESNOTEXIST:
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_1, self.client_2, key, state_2.local_timestamp
                )

            elif state_1.state == SyncState.NOCHANGES and state_2.state == SyncState.DOESNOTEXIST:
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_2, self.client_1, key, state_1.remote_timestamp
                )

            elif state_2.state == SyncState.NOCHANGES and state_1.state == SyncState.DOESNOTEXIST:
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_1, self.client_2, key, state_2.remote_timestamp
                )
            elif state_1.state == SyncState.UPDATED and state_2.state == SyncState.DOESNOTEXIST:
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_2, self.client_1, key, state_1.local_timestamp
                )

            elif state_2.state == SyncState.UPDATED and state_1.state == SyncState.DOESNOTEXIST:
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_2, self.client_1, key, state_1.local_timestamp
                )

            elif (
                state_1.state in (SyncState.DELETED, SyncState.DOESNOTEXIST) and
                state_2.state in (SyncState.DELETED, SyncState.DOESNOTEXIST)
            ):
                # nothing to do, they have already both been deleted/do not exist
                continue

            elif (
                state_1.state == SyncState.UPDATED and
                state_2.state == SyncState.NOCHANGES and
                state_1.remote_timestamp == state_2.remote_timestamp
            ):
                deferred_calls[key] = DeferredFunction(
                    self.update_client, self.client_2, self.client_1, key, state_1.local_timestamp
                )

            elif (
                state_2.state == SyncState.UPDATED and
                state_1.state == SyncState.NOCHANGES and
                state_1.remote_timestamp == state_2.remote_timestamp
            ):
                deferred_calls[key] = DeferredFunction(
                    self.update_client, self.client_1, self.client_2, key, state_2.local_timestamp
                )

            elif (
                state_1.state == SyncState.DELETED and
                state_2.state == SyncState.NOCHANGES and
                state_1.remote_timestamp == state_2.remote_timestamp
            ):
                deferred_calls[key] = DeferredFunction(
                    self.delete_client, self.client_2, key, state_1.remote_timestamp
                )

            elif (
                state_2.state == SyncState.DELETED and
                state_1.state == SyncState.NOCHANGES and
                state_1.remote_timestamp == state_2.remote_timestamp
            ):
                deferred_calls[key] = DeferredFunction(
                    self.delete_client, self.client_1, key, state_2.remote_timestamp
                )

            elif (
                state_1.state == SyncState.DELETED and
                state_2.state == SyncState.CREATED and
                state_1.remote_timestamp == state_2.remote_timestamp
            ):
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_1, self.client_2, key, state_2.local_timestamp
                )

            elif (
                state_2.state == SyncState.DELETED and
                state_1.state == SyncState.CREATED and
                state_1.remote_timestamp == state_2.remote_timestamp
            ):
                deferred_calls[key] = DeferredFunction(
                    self.create_client, self.client_2, self.client_1, key, state_1.local_timestamp
                )

            else:
                unhandled_events[key] = (state_1, state_2)

            self.logger.debug('Action=%s', deferred_calls.get(key))

        return deferred_calls, unhandled_events

    def run_deferred_calls(self, deferred_calls):
        # call everything once we know we can handle all of it
        self.logger.debug('There are %s total deferred calls', len(deferred_calls))
        success = []
        try:
            for key in sorted(deferred_calls.keys()):
                deferred_function = deferred_calls[key]
                try:
                    deferred_function()
                    self.client_1.update_index_entry(key)
                    self.client_2.update_index_entry(key)
                    success.append(key)
                except Exception as e:
                    self.logger.error('An error occurred while trying to update %s: %s', key, e)
        except KeyboardInterrupt:
            self.logger.warning('Session interrupted by Keyboard Interrupt. Cleaning up....')

        if len(deferred_calls) > 0:
            self.logger.info('Flushing Index to Storage')
            self.client_1.flush_index()
            self.client_2.flush_index()
        else:
            self.logger.info('Nothing to update')

        return success

    def get_states(self):
        client_1_actions = self.client_1.get_all_actions()
        client_2_actions = self.client_2.get_all_actions()

        all_keys = set(client_1_actions) | set(client_2_actions)
        self.logger.debug(
            '%s keys in total (%s for %s and %s for %s)',
            len(all_keys),
            len(client_1_actions), self.client_1.get_uri(),
            len(client_2_actions), self.client_2.get_uri()
        )

        DOES_NOT_EXIST = SyncState(SyncState.DOESNOTEXIST, None, None)
        for key in sorted(all_keys):
            action_1 = client_1_actions.get(key, DOES_NOT_EXIST)
            action_2 = client_2_actions.get(key, DOES_NOT_EXIST)
            yield key, action_1, action_2

    def get_deferred_function(self, key, action, to_client, from_client):
        if action.state in (SyncState.UPDATED, SyncState.NOCHANGES):
            return DeferredFunction(
                self.update_client, to_client, from_client, key, action.local_timestamp
            )
        elif action.state == SyncState.CREATED:
            return DeferredFunction(
                self.create_client, to_client, from_client, key, action.local_timestamp
            )
        elif action.state == SyncState.DELETED:
            return DeferredFunction(self.delete_client, to_client, key, action.remote_timestamp)
        else:
            raise ValueError('Unknown action provided', action)

    def create_client(self, to_client, from_client, key, timestamp):
        self.logger.info(
            colored.green('Creating %s (%s => %s)'),
            key, from_client.get_uri(), to_client.get_uri()
        )
        self.move(to_client, from_client, key, timestamp)

    def update_client(self, to_client, from_client, key, timestamp):
        self.logger.info(
            colored.yellow('Updating %s (%s => %s)'),
            key, from_client.get_uri(), to_client.get_uri()
        )
        self.move(to_client, from_client, key, timestamp)

    def move(self, to_client, from_client, key, timestamp):
        sync_object = from_client.get(key)

        with get_progress_bar(sync_object.total_size) as progress_bar:
            to_client.put(key, sync_object, callback=progress_bar.update)

        to_client.set_remote_timestamp(key, timestamp)
        from_client.set_remote_timestamp(key, timestamp)

    def delete_client(self, client, key, remote_timestamp):
        self.logger.info(
            colored.red('Deleting %s on %s'),
            key, client.get_uri()
        )
        client.delete(key)
        client.set_remote_timestamp(key, remote_timestamp)


def get_progress_bar(max_value):
    return tqdm.tqdm(
        total=max_value,
        leave=False,
        ncols=80,
        unit='B',
        unit_scale=True,
        mininterval=0.2,
    )
