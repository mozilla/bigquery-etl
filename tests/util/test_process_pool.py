import logging
import os
import sys

import google.auth
import pathos.multiprocessing
import pytest
from google.auth import impersonated_credentials
from pathos.helpers import mp
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.util.process_pool import (
    IMPERSONATE_ENV_VAR,
    init_worker,
    process_pool,
)

# pathos keeps its pools in a module-level dict keyed by the pool id
POOL_STATE = pathos.multiprocessing._ProcessPool__STATE


class _FakeSourceCreds:
    # impersonated_credentials.Credentials reads universe_domain at construction;
    # `valid` short-circuits the pre-refresh so no network call is made.
    universe_domain = "googleapis.com"
    valid = True


def square(x):
    return x * x


def observe_worker_auth(_):
    """Report the auth state a worker sees."""
    return (
        hasattr(google.auth.default, "_bqetl_original_default"),
        os.environ.get(IMPERSONATE_ENV_VAR),
        logging.root.level,
    )


class TestProcessPool:
    def test_map(self):
        with process_pool(2, 4) as pool:
            assert pool.map(square, [1, 2, 3]) == [1, 4, 9]

    def test_single_task_runs_without_workers(self):
        with process_pool(8, 1) as pool:
            assert not isinstance(pool, ProcessingPool)
            assert pool.map(square, [1, 2, 3]) == [1, 4, 9]

    def test_start_method(self):
        start_method = mp.get_start_method()
        with process_pool(2, 4):
            if sys.platform == "darwin":
                assert mp.get_start_method() == "spawn"
            else:
                assert mp.get_start_method() == start_method

    def test_pools_are_not_shared(self):
        # pools with the same worker count must stay separate, otherwise a
        # caller could get workers that were created before the start method
        # was set, or with a different initializer
        with process_pool(2, 4) as first:
            with process_pool(2, 4) as second:
                assert first._id != second._id
                assert POOL_STATE[first._id] is not POOL_STATE[second._id]

    def test_pool_released_on_exit(self):
        with process_pool(2, 4) as pool:
            pool_id = pool._id
            assert pool_id in POOL_STATE
        assert pool_id not in POOL_STATE

    def test_pool_released_on_error(self):
        with pytest.raises(ValueError):
            with process_pool(2, 4) as pool:
                pool_id = pool._id
                raise ValueError("boom")
        assert pool_id not in POOL_STATE


class TestInitWorker:
    def test_installs_impersonation_from_env(self, monkeypatch):
        monkeypatch.setattr(
            google.auth, "default", lambda *a, **k: (_FakeSourceCreds(), "proj")
        )
        # recorded so monkeypatch restores it after init_worker changes it
        monkeypatch.setattr(logging.root, "level", logging.root.level)
        monkeypatch.setenv(IMPERSONATE_ENV_VAR, "sa@p.iam.gserviceaccount.com")

        init_worker(logging.DEBUG)

        creds, _ = google.auth.default()
        assert isinstance(creds, impersonated_credentials.Credentials)
        assert creds._target_principal == "sa@p.iam.gserviceaccount.com"
        assert logging.root.level == logging.DEBUG

    def test_no_impersonation_without_env(self, monkeypatch):
        # `--no-impersonate` unsets the env var, and workers must not
        # impersonate in that case
        def unimpersonated(*args, **kwargs):
            return (_FakeSourceCreds(), "proj")

        monkeypatch.setattr(google.auth, "default", unimpersonated)
        monkeypatch.setattr(logging.root, "level", logging.root.level)
        monkeypatch.delenv(IMPERSONATE_ENV_VAR, raising=False)

        init_worker(logging.INFO)

        assert google.auth.default is unimpersonated

    def test_pool_workers_impersonate(self, monkeypatch):
        # spawned workers import google.auth fresh, so the pool has to
        # re-install the impersonation wrapper in each one
        monkeypatch.setenv(IMPERSONATE_ENV_VAR, "sa@p.iam.gserviceaccount.com")

        with process_pool(2, 4) as pool:
            observed = pool.map(observe_worker_auth, range(4))

        assert all(wrapped for wrapped, _, _ in observed)
        assert {sa for _, sa, _ in observed} == {"sa@p.iam.gserviceaccount.com"}
        assert {level for _, _, level in observed} == {logging.root.level}
