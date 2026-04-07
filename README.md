# Learning Hub

A personal knowledge base and learning website built to document data engineering topics, architecture patterns, and a small runnable Spark plus Iceberg reference runtime.

## Live Site

> After pushing to GitHub, enable **GitHub Pages** (Settings → Pages → Source: `main` branch, `/ (root)`) and your site will be live at:
> `https://<your-username>.github.io/learning-hub/`

## Structure

```
learning-hub/
├── docker-compose.learning-hub.yml  # Full Spark-Iceberg compose based on your running stack
├── docker/             # Compose snippet and Docker notes for the Spark-Iceberg stack
├── data/               # Repo-local data mount target for the saved compose file
├── index.html          # Landing page with links to all topics
├── ddl/                # Demo Iceberg control-table, staging, seed, and serving SQL
├── notebooks/          # Runnable Jupyter notebook for the demo flow
├── pages/              # Individual content pages
│   └── working-reference-implementation.html
├── spark/              # Metadata-driven Spark runtime for the demo catalog
├── warehouse/          # Repo-local warehouse mount target for the saved compose file
├── README.md
└── .gitignore
```

## Reference Runtime

The site now includes a small metadata-driven runtime aimed at the `demo` Iceberg catalog used in the working reference implementation.

1. Apply `ddl/demo_metadata_framework.sql` to create `demo.ctl`, `demo.bronze`, `demo.silver`, and `demo.gold` plus the control tables.
2. Apply `ddl/demo_staging_sources.sql` to create the five demo source tables under `demo.staging`.
3. Apply `ddl/demo_seed_batch_1001.sql` for the first source-state snapshot.
4. Run `spark/run_pipeline.py` to process active sources.
5. Apply `ddl/demo_seed_batch_1002.sql` to simulate source changes for the second batch.
6. Run `spark/run_pipeline.py` again, or run selected sources only.
7. Apply `ddl/demo_sales_serving.sql` to build a joined sales-serving table from all five gold outputs.
8. Insert replay requests into `demo.ctl.replay_request` and run `spark/replay_runner.py` when you want to test rebuild behavior.

The shared runtime logic lives in `spark/runtime_framework.py`.

## Running In Spark-Iceberg Docker

These steps assume your container service name is `spark-iceberg` and the Iceberg catalog name is `demo`.

The repository now includes:

- `docker-compose.learning-hub.yml` as the saved full compose file based on your shared stack
- `docker/README.md` with the compose notes for this repo setup
- `notebooks/learning_hub_demo.ipynb` as a runnable notebook version of the setup and pipeline flow

Use the saved compose file directly:

```powershell
docker compose -f docker-compose.learning-hub.yml up -d
```

Then open Jupyter at `http://localhost:8888` and run [notebooks/learning_hub_demo.ipynb](notebooks/learning_hub_demo.ipynb).

The compose file already mounts the folders needed by the demo, including:

- `warehouse/`
- `data/`
- `notebooks/`
- `ddl/`
- `spark/`

So there is no separate manual mount step anymore.

## Adding New Content

1. Create a new `.html` file in the `pages/` folder
2. Add a card linking to it in `index.html` inside the `<div id="contentGrid">` section
3. Commit and push — GitHub Pages will update automatically

## Tech

- Pure HTML + CSS (no frameworks, no build step)
- Dark theme with IBM Plex fonts
- Fully responsive
- Hosted free on GitHub Pages
