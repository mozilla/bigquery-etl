"""Generate templated views."""
from pathlib import Path
from argparse import ArgumentParser
from jinja2 import Environment, PackageLoader

from bigquery_etl.format_sql.formatter import reformat


def write_view(dataset_path, template_name, args, **kwargs):
    # load template and get output view name
    env = Environment(loader=PackageLoader("bigquery_etl", "glam/templates"))
    view = env.get_template(f"{template_name}.sql")
    view_name = f"{args.prefix}_{template_name}"

    # create the directory for the view
    (dataset_path / view_name).mkdir(exist_ok=True)
    view_path = dataset_path / view_name / "view.sql"

    # write the query with appropriate variables
    print(f"writing {view_path}")
    with view_path.open("w") as fp:
        data = reformat(view.render(**{**vars(args), **kwargs}))
        print(data, file=fp)


def main():
    """Generate a table with latest version per channel."""
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument("--prefix", default="fenix")
    parser.add_argument("--dataset", default="glam_etl")
    parser.add_argument("--path", default="sql/")
    args = parser.parse_args()

    dataset_path = Path(args.path) / args.dataset
    if not dataset_path.is_dir():
        raise Exception(f"path to {dataset_path} not found")

    write_view(dataset_path, "view_clients_daily_scalar_aggregates_v1", args)
    write_view(dataset_path, "view_clients_daily_histogram_aggregates_v1", args)


if __name__ == "__main__":
    main()
