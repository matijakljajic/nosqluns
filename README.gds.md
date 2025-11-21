# Graph Data Science Guide

The `cypher/gds_workflows.cypher` file is a ready-made set of GDS demos you can run: PageRank on co-medal countries, Node Similarity on regional athletes, and Louvain communities on medal partnerships. Use this README to remember how to execute and customize them.

## Prerequisites

1. Neo4j container is running (`podman compose up -d`).
2. Dataset is loaded (`cypher/seed_nodes.cypher` then `cypher/seed_relationships.cypher`).
3. GDS plugin is enabled by default; verify with:
```cypher
CALL gds.version();
```

## Run the workflows

Execute everything in one go:
```bash
cat cypher/gds_workflows.cypher | podman exec -i neo4j cypher-shell
```
or open the file in Browser and run sections individually. Each workflow starts by dropping the projection it needs, so re-running is safe.

## What's inside
- **Workflow 1 - Country influence (PageRank)**  
  Builds a co-medal network where two countries connect when their representatives get medals at the same event; edge weight = shared medal events. Streams PageRank to highlight delegations that co-medal with many influential partners. You can try filtering to gold-only medals or adjusting the iteration count to discuss convergence.

- **Workflow 2 - Athlete similarity (Node Similarity)**  
  Focuses on athletes from a predefined set of ex-Yu countries. Edge weight = shared events + shared teams. Streams similarities to surface duos that appear together often. You can swap in another country list or change the weight formula to bias events vs teams.

- **Workflow 3 - Country medal clusters (Louvain)**  
  Reuses the co-medal projection to find communities of countries that medal together more than with the rest of the world. Great for talking about modularity and reading community summaries (size + sample members).

## Adaptation guide

1. Define the node set: `RETURN id(n) AS id` for the entities you care about.
2. Define relationships with weights if needed: `RETURN source, target, weight`.
3. Project with `gds.graph.project('name', $nodeQuery, $relQuery, {validateRelationships: false})`.
4. Run the algorithm of your choice (`gds.pageRank.stream`, `gds.nodeSimilarity.stream`, `gds.louvain.stream`, etc.).
5. Drop the temp graph: `CALL gds.graph.drop('name', false);`

## Troubleshooting

- `graph already exists` - drop it first with `CALL gds.graph.drop('<name>', false);`
- `Relationship weight property not found` - ensure your projection query returns a column matching the `relationshipWeightProperty`.
- `Procedure not found` - confirm GDS is loaded and that you are using the correct major version syntax.

For deeper dives, the official GDS docs live at [official Neo4j docs](https://neo4j.com/docs/graph-data-science/current/).
