# -*- coding: utf-8 -*-


def update_client(to_client, from_client, key, timestamp):
    print('UPDATING ', key, 'on', to_client, 'to', from_client, 'version')
    to_client.put(key, from_client.get(key))
    to_client.set_remote_timestamp(key, timestamp)
    from_client.set_remote_timestamp(key, timestamp)


def delete_client(client, key):
    print('DELETING ', key, 'on', client)
    client.delete(key)


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
    deferred_calls = []

    for key, action_1, action_2 in get_actions(client_1, client_2):
        if action_1.action == 'NONE' and action_2.action == 'NONE':
            if action_1.timestamp == action_2.timestamp:
                continue
            elif action_1.timestamp is None and action_2.timestamp:
                deferred_calls.append(
                    (update_client, [client_1, client_2, key, action_2.timestamp])
                )
            elif action_2.timestamp is None and action_1.timestamp:
                deferred_calls.append(
                    (update_client, [client_2, client_1, key, action_1.timestamp])
                )
            elif action_1.timestamp > action_2.timestamp:
                deferred_calls.append(
                    (update_client, [client_2, client_1, key, action_1.timestamp])
                )
            elif action_2.timestamp > action_1.timestamp:
                deferred_calls.append(
                    (update_client, [client_1, client_2, key, action_2.timestamp])
                )

        elif action_1.action == 'UPDATE' and action_2.action == 'NONE':
            deferred_calls.append(
                (update_client, [client_2, client_1, key, action_1.timestamp])
            )

        elif action_2.action == 'UPDATE' and action_1.action == 'NONE':
            deferred_calls.append(
                (update_client, [client_1, client_2, key, action_2.timestamp])
            )

        elif action_1.action == 'DELETE' and action_2.action == 'NONE':
            deferred_calls.append(
                (delete_client, [client_2, key])
            )

        elif action_2.action == 'DELETE' and action_1.action == 'NONE':
            deferred_calls.append(
                (delete_client, [client_1, key])
            )

        elif action_1.action == 'DELETE' and action_2.action == 'DELETE':
            # nothing to do
            continue

        # TODO: Check DELETE timestamp. if it is older than you should be able to safely ignore it

        else:
            raise ValueError(
                'Unhandled state, aborting before anything is updated', key, action_1, action_2
            )

    # call everything once we know we can handle all of it
    # TODO: Should probably catch any exception and update the index anyway here
    for func, args in deferred_calls:
        func(*args)

    client_1.update_index()
    client_2.update_index()
