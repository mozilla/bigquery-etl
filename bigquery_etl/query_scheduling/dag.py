"""Represents an Airflow DAG."""


class DagParseException(Exception):
    """Raised when DAG config is invalid."""

    def __init__(self, message):
        """Throw DagParseException."""
        message = f"""
        {message}

        Expected yaml format:
        name:
            schedule_interval: string,
            default_args: map
        """

        super(DagParseException, self).__init__(message)


class InvalidDag(Exception):
    """Raised when the resulting DAG is invalid."""

    pass


class Dag:
    """Representation of a DAG configuration."""

    def __init__(self, name, schedule_interval, default_args):
        """Instantiate new DAG representation."""
        self.name = name
        self.schedule_interval = schedule_interval
        self.default_args = default_args
        self.tasks = []

    def add_tasks(self, tasks):
        """Add tasks to be scheduled as part of the DAG."""
        self.tasks += tasks

    @classmethod
    def from_dict(cls, d):
        """
        Parse the DAG configuration from a dict and create a new Dag instance.

        Expected dict format:
        {
            "name": {
                "schedule_interval": string,
                "default_args": dict
            }
        }
        """
        if len(d.keys()) != 1:
            raise DagParseException(f"Invalid DAG name in {d}.")

        name = list(d.keys())[0]

        if "schedule_interval" not in d[name]:
            raise DagParseException(f"schedule_interval missing in {d}.")

        schedule_interval = d[name]["schedule_interval"]

        # todo: check format - either cron or daily, ....
        # airflow dag validation might catch that, if not check here

        default_args = d.get("default_args", {})

        return cls(name, schedule_interval, default_args)
