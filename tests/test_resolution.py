# -*- encoding: utf-8 -*-

import pytest

from s4.clients import SyncState, local, s3
from s4.resolution import Resolution


class TestResolution(object):
    def test_equal_wrong_instance(self):
        resolution = Resolution(Resolution.CREATE, None, None, 'bar', 23232)
        assert resolution != "Not a Resolution"

    def test_equal_to_self(self):
        resolution = Resolution(Resolution.UPDATE, None, None, 'fffff', 232323)
        assert resolution == resolution

    def test_equal(self):
        resolution_1 = Resolution(Resolution.UPDATE, None, None, 'fffff', 232323)
        resolution_2 = Resolution(Resolution.UPDATE, None, None, 'fffff', 232323)
        assert resolution_1 == resolution_2

    def test_not_equal(self):
        resolution_1 = Resolution(Resolution.UPDATE, None, None, 'fffff', 232323)
        resolution_2 = Resolution(Resolution.DELETE, None, None, 'wew', 3823)
        assert resolution_1 != resolution_2

    def test_repr(self):
        s3_client = s3.S3SyncClient(None, 'mortybucket', 'dimensional/portals')
        local_client = local.LocalSyncClient('/home/picklerick')
        resolution = Resolution(Resolution.CREATE, s3_client, local_client, 'foo', 20023)
        expected_repr = (
            "Resolution<action=CREATE, "
            "to=s3://mortybucket/dimensional/portals/, "
            "from=/home/picklerick/, "
            "key=foo, timestamp=20023>"
        )
        assert repr(resolution) == expected_repr

    @pytest.mark.parametrize(['state', 'action'], [
        (SyncState.UPDATED, Resolution.UPDATE),
        (SyncState.UPDATED, Resolution.UPDATE),
        (SyncState.CREATED, Resolution.CREATE),
        (SyncState.DELETED, Resolution.DELETE),
    ])
    def test_get_resolution_updated(self, state, action, s3_client, local_client):
        sync_state = SyncState(state, 1234, 4567)
        resolution = Resolution.get_resolution(
            'foo/bar', sync_state, s3_client, local_client
        )
        assert resolution.action == action
        assert resolution.to_client == s3_client
        assert resolution.key == 'foo/bar'
        if state != SyncState.DELETED:
            assert resolution.timestamp == 1234
            assert resolution.from_client == local_client
        else:
            assert resolution.timestamp == 4567
            assert resolution.from_client is None

    def test_invalid_sync_state(self):
        with pytest.raises(ValueError) as exc:
            Resolution.get_resolution('', SyncState('Invalid', None, None), None, None)
        assert exc.value
