import os
import stat
import sys

import pytest

from bigquery_etl.util.cache_dir import get_private_cache_dir

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX permissions only"
)


class TestGetPrivateCacheDir:
    @pytest.fixture(autouse=True)
    def tmp_tempdir(self, monkeypatch, tmp_path):
        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))

    def test_creates_private_dir(self, tmp_path):
        cache_dir = get_private_cache_dir("test_cache")

        assert cache_dir == tmp_path / "test_cache"
        assert stat.S_IMODE(os.stat(cache_dir).st_mode) == 0o700

    def test_tightens_permissions_of_own_dir(self, tmp_path):
        (tmp_path / "test_cache").mkdir(mode=0o755)
        os.chmod(tmp_path / "test_cache", 0o755)

        cache_dir = get_private_cache_dir("test_cache")

        assert cache_dir == tmp_path / "test_cache"
        assert stat.S_IMODE(os.stat(cache_dir).st_mode) == 0o700

    def test_rejects_dir_writable_by_others(self, tmp_path):
        (tmp_path / "test_cache").mkdir()
        os.chmod(tmp_path / "test_cache", 0o777)

        assert get_private_cache_dir("test_cache") is None

    def test_rejects_dir_owned_by_other_user(self, monkeypatch):
        monkeypatch.setattr("os.getuid", lambda: os.stat("/").st_uid + 12345)

        assert get_private_cache_dir("test_cache") is None

    def test_rejects_symlink(self, tmp_path):
        (tmp_path / "target").mkdir(mode=0o700)
        (tmp_path / "test_cache").symlink_to(tmp_path / "target")

        assert get_private_cache_dir("test_cache") is None
