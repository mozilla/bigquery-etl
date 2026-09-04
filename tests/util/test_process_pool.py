import sys

import pathos.multiprocessing
import pytest
from pathos.helpers import mp

from bigquery_etl.util.process_pool import process_pool

# pathos keeps its pools in a module-level dict keyed by the pool id
POOL_STATE = pathos.multiprocessing._ProcessPool__STATE


def square(x):
    return x * x


class TestProcessPool:
    def test_map(self):
        with process_pool(2, "test_map") as pool:
            assert pool.map(square, [1, 2, 3]) == [1, 4, 9]

    def test_start_method(self):
        start_method = mp.get_start_method()
        with process_pool(2, "test_start_method"):
            if sys.platform == "darwin":
                assert mp.get_start_method() == "spawn"
            else:
                assert mp.get_start_method() == start_method

    def test_pool_ids_are_not_shared(self):
        # pools with the same worker count but different ids must stay separate,
        # otherwise a caller could get workers that were created before the
        # start method was set
        with process_pool(2, "test_ids_a"):
            with process_pool(2, "test_ids_b"):
                assert "test_ids_a" in POOL_STATE
                assert "test_ids_b" in POOL_STATE
                assert POOL_STATE["test_ids_a"] is not POOL_STATE["test_ids_b"]

    def test_pool_released_on_exit(self):
        with process_pool(2, "test_release"):
            assert "test_release" in POOL_STATE
        assert "test_release" not in POOL_STATE

    def test_pool_released_on_error(self):
        with pytest.raises(ValueError):
            with process_pool(2, "test_error"):
                raise ValueError("boom")
        assert "test_error" not in POOL_STATE
