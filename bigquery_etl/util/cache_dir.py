"""Helpers for local cache directories."""

import os
import stat
import tempfile
from pathlib import Path
from typing import Optional


def get_private_cache_dir(name: str) -> Optional[Path]:
    """Return a cache directory under the temp dir that only the current user can access.

    Cache files are deserialized with pickle, so they must not be writable by
    other users. On a shared temp dir (e.g. /tmp on multi-user Linux hosts)
    another user could pre-create the directory. If the existing directory
    isn't owned by the current user, or is writable by group/others, None is
    returned and caching should be skipped.
    """
    cache_dir = Path(tempfile.gettempdir()) / name
    try:
        cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        st = os.lstat(cache_dir)
    except OSError as e:
        print(f"[CACHE] Unable to create cache directory {cache_dir}: {e}")
        return None

    if not stat.S_ISDIR(st.st_mode):
        print(f"[CACHE] {cache_dir} is not a directory, skipping cache")
        return None

    # ownership and permission bits aren't meaningful on Windows
    if hasattr(os, "getuid"):
        if st.st_uid != os.getuid():
            print(f"[CACHE] {cache_dir} is not owned by current user, skipping cache")
            return None
        if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            print(f"[CACHE] {cache_dir} is writable by other users, skipping cache")
            return None
        # no one else could have written to it, so it's safe to tighten
        # permissions (e.g. a directory restored with default permissions in CI)
        if stat.S_IMODE(st.st_mode) != 0o700:
            try:
                os.chmod(cache_dir, 0o700)
            except OSError:
                pass

    return cache_dir
