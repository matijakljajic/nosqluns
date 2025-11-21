# Cypher Guide

Use this as the Neo4j cheat sheet. If the stack is running and seeded ([`README.neo4j.md`](README.neo4j.md)), you can copy-paste anything here into Browser or `cypher-shell`.

## Running queries

- Shell: `podman exec -it neo4j cypher-shell` (swap `podman` for `docker` if needed).
- Browser: `http://localhost:7474`, connect with `neo4j://neo4j@localhost:7687` (blank password unless you enabled auth).
- Handy files under `cypher/`:
  - `simple_queries.cypher`: warm-up counts and basic traversals.
  - `complex_queries.cypher`: analytical examples with subqueries/collections.
  - `gds_workflows.cypher`: PageRank, Node Similarity, and Louvain demos (see [`README.gds.md`](README.gds.md)).

## Cypher in 90 seconds

- Think in **patterns**, not tables: `(a:Athlete)-[:REPRESENTS]->(c:Country)`
- Clause pipeline (top executes first): `MATCH/OPTIONAL MATCH -> WHERE -> WITH -> ORDER BY/SKIP/LIMIT -> RETURN`
- `WITH` is your friend: rename columns, filter on aggregates, break pipelines.
- Writes: `MERGE` (find or create), `CREATE` (always create), `SET` (update properties/labels).
- `UNWIND` turns a list into rows; subqueries (`CALL { ... }`) encapsulate logic and support batching.

Small reminders:
```cypher
MATCH (a:Athlete)
WHERE EXISTS { MATCH (a)-[:WON_MEDAL]->(:Event) }
RETURN a.name
```
```cypher
MATCH (c:Country)-[:WON_MEDAL]->(e:Event)
WITH c, count(*) AS medals
WHERE medals >= 5
RETURN c.name, medals;
```
