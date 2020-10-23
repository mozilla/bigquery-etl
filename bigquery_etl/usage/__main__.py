import pathlib
import yaml

from jinja2 import Template

from bigquery_etl.format_sql.formatter import reformat


HERE = pathlib.Path(__file__).parent.absolute()
ROOT = pathlib.Path(__file__).parent.parent.parent.absolute()


with (HERE / "config" / "measures.yaml").open("r") as f:
    data = yaml.safe_load(f)

with (HERE / "templates" / "view_last_seen.sql").open("r") as f:
    template = Template(f.read())

for source in data["sources"]:
    print(reformat(template.render(**source)))
