# "NoSQL" Course Neo4j Project

Neo4j database utilizing the [Paris 2024 Olympics dataset](https://www.kaggle.com/datasets/piterfm/paris-2024-olympic-summer-games). It is meant to serve as a reference implementation for the Neo4j assignment in the NoSQL course.

## Quick start

1) Start Neo4j (Podman or Docker):
```bash
podman compose up -d
```

2) Seed the data:
```bash
cat cypher/seed_nodes.cypher | podman exec -i neo4j cypher-shell
cat cypher/seed_relationships.cypher | podman exec -i neo4j cypher-shell
```

3) Explore:
- Neo4j Browser: `http://localhost:7474` (no auth by default).
- Copy-paste queries from the `cypher/` dir into the Browser or run the `.cypher` files directly.
- GDS examples live in `cypher/gds_workflows.cypher` and are explained in [`README.gds.md`](README.gds.md).

Docker users: swap `podman` for `docker` in the commands above.

## Documentation map

- [`README.neo4j.md`](README.neo4j.md): running the stack, seeding, maintenance, troubleshooting.
- [`README.dataset.md`](README.dataset.md): graph model, entity quirks, and regeneration notes.
- [`README.cypher.md`](README.cypher.md): Cypher cheatsheet.
- [`README.gds.md`](README.gds.md): how to run and adapt the bundled GDS workflows.

## Repository layout

- `cypher/`: seed scripts (`seed_nodes.cypher`, `seed_relationships.cypher`) and example queries (`simple_queries.cypher`, `complex_queries.cypher`, `gds_workflows.cypher`).
- `import/`: normalized CSVs mounted into Neo4j for `LOAD CSV` imports.
- `util/`: helper script `normalize_dataset.py` that rebuilds normalized `import/`.
- `docker-compose.yml`: single Neo4j service with volumes for data/logs/config/plugins.

# License

Code and queries are licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Modified and normalized dataset in the `import/` dir is under the [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
