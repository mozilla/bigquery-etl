from pathlib import Path
from .utils import ndjson_load

data_root = Path(__file__).parent.parent / "data"


def qualify(project, dataset, table):
    return f"{project}:{dataset}.{table}"


if __name__ == "__main__":
    refs = data_root.glob("*/views_references.ndjson")
    edge_list = []
    nodes = set()
    for ref in refs:
        project = ref.parent.name
        print(project)
        data = ndjson_load(ref)
        # write the schema explicitly somewhere for reference?
        for entry in data:
            dest = qualify(entry["projectId"], entry["datasetId"], entry["tableId"])
            for referenced in entry["query"].get("referencedTables", []):
                source = qualify(
                    referenced["projectId"],
                    referenced["datasetId"],
                    referenced["tableId"],
                )
                edge_list.append([source, dest])
                nodes.add(source)
                nodes.add(dest)

    node_map = {name: uid for uid, name in enumerate(nodes)}

    with (data_root / "edgelist.csv").open("w") as fp:
        for edge in sorted(edge_list):
            fp.write(f"{node_map[edge[0]]},{node_map[edge[1]]}\n")
