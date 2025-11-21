# Dataset & Model (Paris 2024 Olympics)

This repository ships a normalized, Neo4j-ready snapshot of the [Paris 2024 Olympics results feed](https://www.kaggle.com/datasets/piterfm/paris-2024-olympic-summer-games).

- CSVs live in `import/` and already follow Neo4j bulk import conventions (`:ID`, `:START_ID`, `:END_ID`, `:TYPE`).
- If you reintroduce the raw Kaggle feeds, `python util/normalize_dataset.py` rebuilds `import/` from scratch.
- Seed scripts in `cypher/` (`seed_nodes.cypher` and `seed_relationships.cypher`) load everything into Neo4j.

## Graph at a glance

| Label | Count | What it represents |
| --- | --- | --- |
| `Country` | 225 | IOC / NOC catalog for nationality + representation. |
| `Sport` | 47 | Disciplines (Athletics, Wrestling, ...). |
| `Event` | 330 | Competition events inferred from results. |
| `Venue` | 43 | Official venues, linked to sports and sessions. |
| `Session` | 3896 | Scheduled instances with start/end, phase, medal flag, venue. |
| `Athlete` | 11,113 | Athlete roster with demographics + disciplines. |
| `Coach` | 975 | Team staff with category/function. |
| `Team` | 1698 | National squads per discipline/event. |
| `Official` | 1,021 | Technical officials and federation assignments. |

Visualize the schema in Browser:
```cypher
CALL db.schema.visualization();
```

### Core relationships

- Representation: `Country<-[:REPRESENTS]-{Athlete|Coach|Team|Official}`
- Competitions: `Athlete|Team-[:COMPETED_IN]->Event` (rank/result metadata on the relationship)
- Medals: `Athlete|Team-[:WON_MEDAL]->Event` (medal type and ceremony date on the relationship)
- Roster: `Athlete-[:MEMBER_OF]->Team`, `Coach-[:COACHES]->Team`
- Schedule & venues: `Session-[:HELD_AT]->Venue`, `Session-[:SESSION_FOR]->Sport`, `Event-[:HOSTED_AT]->Venue`

### Quirks

- **Events vs sessions**: event IDs come from the results feed; sessions come from official schedules. They meet via the venue (`Event-[:HOSTED_AT]->Venue<-[:HELD_AT]-Session`).
- **Teams**: some medal entries lack a team code; medals fall back to athlete-level edges so athlete data is complete even when teams are sparse.
- **Venues**: missing venue codes get slugged (`anon-#`) so schedule rows are preserved; known venues keep official slugs and URLs.
- **Officials**: federation codes match to NOCs when possible; otherwise they stay as attributes without a `REPRESENTS` edge.

## Import paths

Choose your favorite way to ingest:

1) **Neo4j admin import** (fastest for a fresh DB):
```bash
neo4j-admin database import full neo4j \
  --nodes=import/nodes_*.csv \
  --relationships=import/rels_*.csv
```
2) **Seed scripts** (already used in this repo):
```bash
cat cypher/seed_nodes.cypher | podman exec -i neo4j cypher-shell
cat cypher/seed_relationships.cypher | podman exec -i neo4j cypher-shell
```
The scripts create constraints, coerce types, and batch the loads.

## Regenerating the dataset

```bash
python util/normalize_dataset.py
```

Rebuilds every CSV under `import/` (overwrites existing files).

## Example query

```cypher
MATCH (a:Athlete)-[r:COMPETED_IN]->(e:Event)-[:HOSTED_AT]->(v:Venue)
WHERE r.rank <= 3 AND v.name CONTAINS "Arena"
RETURN a.name AS athlete,
       e.name + " " + e.sport_name AS event,
       r.rank AS finish_position,
       v.name AS venue
ORDER BY r.rank;
```

For more recipes, jump to [`README.cypher.md`](README.cypher.md).
