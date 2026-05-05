"""Threaded TopologialSorter."""

import multiprocessing
from copy import deepcopy
from graphlib import TopologicalSorter
from typing import Dict, Set


class ParallelTopologicalSorter:
    """TopologicalSorter that processes sorted items concurrently."""

    def __init__(
        self,
        dependencies: Dict[str, Set[str]],
        parallelism: int = 8,
        with_follow_up=False,
    ):
        """Initialize."""
        self.dependencies = deepcopy(dependencies)
        self.parallelism = parallelism

        manager = multiprocessing.Manager()
        self.visited = manager.dict({dep: False for dep, _ in dependencies.items()})
        self.with_follow_up = with_follow_up

    def _worker(
        self, task_queue, finalized_tasks_queue, followup_queue, visited_map, callback
    ):
        while True:
            item = task_queue.get()
            callback(item, followup_queue)
            if item in visited_map:
                visited_map[item] = True
            finalized_tasks_queue.put(item)
            task_queue.task_done()

    def map(self, callback):
        """Sort and process dependencies."""
        task_queue = multiprocessing.JoinableQueue()
        finalized_tasks_queue = multiprocessing.JoinableQueue()
        processes = []

        if self.with_follow_up:
            followup_queue = multiprocessing.Queue()
        else:
            followup_queue = None

        for _ in range(self.parallelism):
            process = multiprocessing.Process(
                target=self._worker,
                args=(
                    task_queue,
                    finalized_tasks_queue,
                    followup_queue,
                    self.visited,
                    callback,
                ),
                daemon=True,
            )
            process.start()
            processes.append(process)

        # pallel sorting and processing
        ts = TopologicalSorter(self.dependencies)
        ts.prepare()

        while ts.is_active():
            # queue task to process
            for task in ts.get_ready():
                task_queue.put(task)

            # task done
            task = finalized_tasks_queue.get()
            ts.done(task)
            finalized_tasks_queue.task_done()

        task_queue.join()
        finalized_tasks_queue.join()

        # followup items cannot be added to the topological sorter after prepare() is called
        # processing them here sequentially
        if followup_queue:
            while not followup_queue.empty():
                follow_up_item = followup_queue.get()
                callback(follow_up_item, followup_queue)

        # check that all dependencies have been processed
        for task, _ in self.visited.items():
            assert self.visited[task]
