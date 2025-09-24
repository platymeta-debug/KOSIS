from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.io_utils import load_irf_csv, load_lp_betas, read_yaml, save_csv, save_md
from src.scenario_dsl import validate_and_expand
from src.scenario_engine import apply_irf, summarize_effects
from src.scenario_post import render_markdown, sectionize


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", required=True, help="YAML 파일 경로")
    ap.add_argument("--irf", default="out_causal/irf_svar.csv", help="IRF csv (없으면 irf_var.csv 사용)")
    ap.add_argument("--fallback-irf", default="out_causal/irf_var.csv")
    ap.add_argument("--lp", default="out_causal/lp_betas.csv")
    ap.add_argument("--outdir", default="out_scenario")
    ap.add_argument("--summary-h", type=int, default=4)
    args = ap.parse_args()

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    spec = read_yaml(args.scenario)
    scn = validate_and_expand(spec)
    name = scn.get("meta", {}).get("name", os.path.basename(args.scenario))

    irf_path = args.irf if os.path.exists(args.irf) else args.fallback_irf
    irf_df = load_irf_csv(irf_path)

    eff_df = apply_irf(irf_df, scn)
    save_csv(eff_df, os.path.join(args.outdir, "effects_full.csv"))

    summ = summarize_effects(eff_df, horizon_pick=args.summary_h)
    save_csv(summ, os.path.join(args.outdir, f"summary_h{args.summary_h}.csv"))

    sect = sectionize(eff_df, h_pick=args.summary_h)
    for sec, df in sect.items():
        save_csv(df, os.path.join(args.outdir, f"{sec.lower()}_h{args.summary_h}.csv"))

    lp_df = load_lp_betas(args.lp)
    if lp_df is not None:
        save_csv(lp_df, os.path.join(args.outdir, "lp_betas_copy.csv"))

    md = render_markdown(name, args.summary_h, sect)
    save_md(os.path.join(args.outdir, "report.md"), md)
    print(f"[scenario] '{name}' done → {args.outdir}")


if __name__ == "__main__":
    main()
