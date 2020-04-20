from itertools import groupby
import yaml


class DagParseException(Exception):
    """Raised when DAG config is invalid."""

    def __init__(self, message, errors):
        message = f"""
        {message}

        Expected yaml format:
        name: 
            schedule_interval: string,
            default_args: map
        """

        super(DagParseException, self).__init__(message)


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
        if d.keys() != 1:
            raise DagParseException(f"Invalid DAG name in {d}.")

        name = d.keys()[0]

        if "schedule_interval" not in d[name]:
            raise DagParseException(f"schedule_interval missing in {d}.")

        schedule_interval = d[name][schedule_interval]

        # todo: check format - either cron or daily, ....
        # airflow dag validation might catch that, if not check here

        default_args = d.get("default_args", {})

        return cls(name, schedule_interval, default_args)


class Dags:
    """Representation of all configured DAGs."""

    def __init__(self, dags):
        """Instantiate DAGs."""
        self.dags = dags

    @classmethod
    def from_dict(cls, d):
        """
        Parse DAG configurations from a dict and create new instances.

        Expected dict format:
        {
            "dag_name1": {
                "schedule_interval": string,
                "default_args": dict
            },
            "dag_name2": {
                "schedule_interval": string,
                "default_args": dict
            },
            ...          
        }
        """

        dags = [Dag.from_dict({k: v}) for k, v in d.items()]
        return cls(dags)

    @classmethod
    def from_file(cls, config_file):
        with open(config_file, "r") as yaml_stream:
            dags_config = yaml.safe_load(yaml_stream)
            return Dags.from_dict(dags_config)

    def dag_by_name(self, name):
        for dag in self.dags:
            if dag.name == name:
                return dag

        return None

    def with_tasks(self, tasks):
        """Assign tasks to their corresponding DAGs."""
        for dag_name, tasks in groupby(tasks, lambda t: t.dag_name):
            dag = self.dag_by_name(dag_name)

            if dag is None:
                print(
                    f"Warning: DAG {dag_name} does not exist in dags.yaml"
                    "but used in task definition {tasks[0].name}."
                )
            else:
                dag.add_tasks(tasks)

    def to_airflow_dags(self):
        """Write DAG representation as Airflow dags to file."""
        # todo
        pass
