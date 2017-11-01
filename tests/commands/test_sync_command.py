#! -*- encoding: utf-8 -*-
import mock

from s4.clients import SyncState
from s4.commands import sync_command
from s4.resolution import Resolution
from s4.sync import SyncWorker

from tests.utils import FakeInputStream, set_local_contents


@mock.patch('s4.utils.get_input')
class TestHandleConflict(object):
    def test_first_choice(self, get_input, s3_client, local_client):
        get_input.return_value = '1'

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = sync_command.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result.action == Resolution.UPDATE

    def test_second_choice(self, get_input, s3_client, local_client):
        get_input.return_value = '2'

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = sync_command.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result.action == Resolution.DELETE

    def test_skip(self, get_input, s3_client, local_client):
        get_input.return_value = 'X'

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = sync_command.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result is None

    @mock.patch('s4.commands.sync_command.show_diff')
    def test_diff(self, show_diff, get_input, s3_client, local_client):
        get_input.side_effect = FakeInputStream([
            'd',
            'X',
        ])

        action_1 = SyncState(
            SyncState.UPDATED,
            1111, 2222
        )
        action_2 = SyncState(
            SyncState.DELETED,
            3333, 4444
        )

        result = sync_command.handle_conflict(
            'movie',
            action_1, s3_client,
            action_2, local_client,
        )
        assert result is None
        assert show_diff.call_count == 1
        show_diff.assert_called_with(s3_client, local_client, 'movie')


def test_progressbar_smoketest(s3_client, local_client):
    # Just test that nothing blows up
    set_local_contents(
        local_client, 'history.txt',
        data='a long long time ago',
        timestamp=5000,
    )

    worker = SyncWorker(
        s3_client,
        local_client,
        start_callback=sync_command.display_progress_bar,
        update_callback=sync_command.update_progress_bar,
        complete_callback=sync_command.hide_progress_bar,
    )
    worker.sync()
