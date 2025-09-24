from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.report6_assembler import assemble
from src.report6_io import (
    load_irf,
    load_lp,
    load_scenario_effects,
    load_signals,
    load_stationarity,
)
from src.report6_render import render_html, render_markdown


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", default="out_signals.csv")
    parser.add_argument("--irf-svar", dest="irf_svar", default="out_causal/irf_svar.csv")
    parser.add_argument("--irf-var", dest="irf_var", default="out_causal/irf_var.csv")
    parser.add_argument("--lp", default="out_causal/lp_betas.csv")
    parser.add_argument("--stationarity", default="out_causal/stationarity_report.csv")
    parser.add_argument("--scenario-dir", default="out_scenario")
    parser.add_argument("--outdir", default="out_report")
    parser.add_argument("--h-pick", type=int, default=4, help="시나리오 요약 시차(h)")
    args = parser.parse_args()

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    pairs = load_signals(args.signals)
    irf = load_irf(args.irf_svar, args.irf_var)
    lp = load_lp(args.lp)
    _stationarity = load_stationarity(args.stationarity)
    scenario = load_scenario_effects(args.scenario_dir)

    bundle = assemble(pairs, irf, lp, scenario, h_pick=args.h_pick)

    md_path = os.path.join(args.outdir, "report.md")
    render_markdown(bundle, md_path)
    render_html(md_path, os.path.join(args.outdir, "report.html"))

    print(f"[report] done → {args.outdir}/report.md (and .html)")


if __name__ == "__main__":
    main()
