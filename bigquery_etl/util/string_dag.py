"""String Dag."""
from contextlib import contextmanager
from copy import deepcopy
from queue import Empty, Queue
from threading import Lock
from typing import Dict, Set


class StringDag:
    """Abstraction for using a queue to iterate in parallel over a set of values.

    All dependencies must be acked before a given value is added to the queue. Values in
    the queue may be processed in parallel in any order.
    """

    def __init__(self, dependencies: Dict[str, Set[str]]):
        """Initialize."""
        self.dependencies = deepcopy(dependencies)
        self.lock: Lock = Lock()
        self.q: Queue = Queue()
        self.ready: Set[str] = set()
        # ack a nonexisting value to initialize queue
        self._ack(None)

    def _ack(self, ack_value):
        with self.lock:
            for value, deps in self.dependencies.items():
                deps.discard(ack_value)
                if not deps and value not in self.ready:
                    self.ready.add(value)
                    self.q.put(value)

    @contextmanager
    def get(self):
        """Context manager for getting a ready value from the queue."""
        value = self.q.get()
        try:
            yield value
        finally:
            self._ack(value)

    def validate(self):
        """Validate all dependencies eventually resolve."""
        _dag = StringDag(self.dependencies)
        for _ in range(len(self.dependencies)):
            try:
                value = _dag.q.get_nowait()
                _dag._ack(value)
            except Empty:
                raise ValueError(
                    "DAG not valid, some values will never be ready:"
                    f" {_dag.dependencies.keys() - _dag.ready}"
                )
