# Neo4j Stack Guide

Use this when you need to boot the stack, reseed the graph, or just remember how to interact with Neo4j. Dataset and query details live in [`README.dataset.md`](README.dataset.md), [`README.cypher.md`](README.cypher.md), and [`README.gds.md`](README.gds.md).

## Stack anatomy

- One `neo4j:latest` container defined in `docker-compose.yml`.
- Volumes (`neo4j-data`, `neo4j-logs`, `neo4j-config`, `neo4j-plugins`) persist data/config/logs/plugins between runs.
- `./import` is bind-mounted to `/var/lib/neo4j/import`, exposing the normalized CSVs and `.cypher` files.
- Auth is off by default (`NEO4J_AUTH=none`). Set `NEO4J_AUTH=neo4j/<password>` if you want credentials.
- Plugins enabled: APOC + Graph Data Science.

## Run / stop

```bash
# Start
podman compose up -d

# Stop and keep data
podman compose down

# Stop and wipe everything (only if you want a clean slate)
podman compose down -v
```

Docker users: replace `podman` with `docker`.

Endpoints: Browser at `http://localhost:7474`, Bolt at `bolt://localhost:7687`.

## Seed the graph

1) Ensure the CSVs exist (regenerate with `python util/normalize_dataset.py` if you have fresh raw feeds).
2) Load nodes:
```bash
cat cypher/seed_nodes.cypher | podman exec -i neo4j cypher-shell
```
3) Load relationships:
```bash
cat cypher/seed_relationships.cypher | podman exec -i neo4j cypher-shell
```

Both scripts batch with `CALL { ... } IN TRANSACTIONS OF 1000 ROWS` and use `MERGE`, so they are safe to re-run. Quick spot checks:
```cypher
MATCH (a:Athlete) RETURN count(a);
MATCH ()-[r:COMPETED_IN]->() RETURN count(r);
MATCH (s:Session {medal_session: true}) RETURN count(s);
```

If you enable auth, append `-u neo4j -p <password>` to `cypher-shell` calls.

## Querying

Explained in [`README.cypher.md`](README.cypher.md).

Feel free to add your own `.cypher` files under `cypher/` and stream them the same way.

## Maintenance cheat sheet

- Refresh data: rerun `python util/normalize_dataset.py`, then both seed scripts.
- Backups: inside the container run `neo4j-admin database dump neo4j` and copy the dump from the `neo4j-data` volume.
- Logs: check `neo4j-logs` (`debug.log` is most useful if a seed fails).
- Config tweaks: edit files under `neo4j-config` or extend env vars in `docker-compose.yml`.

## Troubleshooting

| Symptom | Likely fix |
| --- | --- |
| `The client is unauthorized due to authentication failure.` | You turned on auth—pass `-u neo4j -p <password>` to `cypher-shell`. |
| `Couldn't load the external resource at: file:///...` | Ensure the CSV lives under `./import` so the container can see it. |
| Duplicate nodes/relationships | Seeds are idempotent; re-run them. |
| Imports feel slow | Restart the container or, if comfortable, raise the transaction batch size inside the scripts. |

If things look cursed, `podman compose down -v`, start again, and re-run the seeds.
