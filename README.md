# KOSIS Macro→Investment Inference MVP

This is a minimal working prototype that:
- Ingests time series (KOSIS via API client **stub** or local CSV mock).
- Standardizes and transforms series.
- Builds a **Correlation Atlas** (rolling correlation + Granger causality).
- Fits a VAR model and computes **IRF**.
- Runs a **Scenario Simulation** from a YAML DSL (e.g., population -10%, policy_rate +50bp).
- Maps macro shocks to **investment insights** (stub mapping) and produces a **markdown report**.
- Includes a **Streamlit dashboard** skeleton.

> Plug in your KOSIS API key and series IDs to use real data.
> For demo, it uses `data/mock_timeseries.csv` (synthetic).

## Quickstart

```bash
# 1) Create venv and install
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2) Run demo (uses mock data)
python main.py --use-mock --report out/report.md

# 3) Scenario example
python main.py --use-mock --scenario scenarios/example.yaml --report out/scenario_report.md

# 4) Launch dashboard (skeleton)
streamlit run src/dashboard.py
```

## Configure real KOSIS data
- Edit `src/kosis_client.py` and set `KOSIS_API_KEY` or export env var `KOSIS_API_KEY`.
- Put your series list into `series.yaml` with columns: `kosis_id`, `name`, `freq`, `transform`.

## Outputs
- `out/report.md`: Macro conclusions + investment table
- `out/atlas/`: heatmaps and tables
- `out/irf/`: IRF charts
- `out/scenario/`: Scenario results

