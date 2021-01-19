import { DataSet } from "vis-data/peer";

function getNode(data, title) {
  return data.nodes.get({
    filter: (item) => item.title == title,
  })[0];
}

// view-source:https://visjs.github.io/vis-network/examples/network/exampleApplications/loadingBar.html
function getDatasetId(name) {
  return name.split(".")[0];
}

function transform(edges, includeDatasetNodes = false) {
  let nodes = new Set();
  for (let i = 0; i < edges.length; i++) {
    nodes.add(edges[i].destination_table);
    nodes.add(edges[i].referenced_table);
  }
  // datasets are nodes too now, but assigned to a different group
  let datasets = new Set();
  nodes.forEach((name) => {
    datasets.add(getDatasetId(name));
  });

  // check for intersection, which will break network visualization
  let intersect = new Set([...nodes].filter((n) => datasets.has(n)));
  if (intersect.size > 0) {
    console.log("intersection between nodes and datasets found");
    console.log(intersect);
  }

  let nodeMap = new Map([...nodes, ...datasets].map((el, idx) => [el, idx]));
  return {
    nodes: new DataSet(
      [...nodes]
        .map((el) => ({
          id: nodeMap.get(el),
          // we show the name without the project prefix to reduce clutter
          label: el.split(":")[1],
          group: 0,
          title: el,
        }))
        .concat(
          includeDatasetNodes
            ? [...datasets].map((el) => ({
                id: nodeMap.get(el),
                label: el,
                group: 1,
                title: el,
              }))
            : []
        )
    ),
    edges: new DataSet(
      edges
        .map((el) => ({
          from: nodeMap.get(el.referenced_table),
          to: nodeMap.get(el.destination_table),
        }))
        .concat(
          includeDatasetNodes
            ? [...nodes].map((el) => ({
                from: nodeMap.get(getDatasetId(el)),
                to: nodeMap.get(el),
              }))
            : []
        )
    ),
  };
}

export { transform, getNode };
