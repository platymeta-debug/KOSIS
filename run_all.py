# run_all.py — one-click pipeline with graceful fallback
import subprocess, sys, os, shutil
from pathlib import Path
from textwrap import dedent

PY = "python"  # venv에서 실행하면 .venv/Scripts/python.exe 가 이 파일을 실행 중일 것

def run(cmd):
    print("\n▶", " ".join(cmd))
    r = subprocess.run(cmd)
    return r.returncode

def ensure_env():
    # .env의 KOSIS_API_KEY 체크(이미 python-dotenv 로드하도록 패치했지만, 친절 메시지)
    if not os.getenv("KOSIS_API_KEY"):
        # .env 직접 읽어 키가 있는지 힌트 제공
        env_path = Path(".env")
        if env_path.exists():
            print("[env] .env detected. If key not loaded, src/config.py uses dotenv to load it.")
        else:
            print("[env] .env not found. Create .env with KOSIS_API_KEY=...  (or export env var)")
    return True

def write_catalog_template(path="series_catalog.csv"):
    tpl = dedent("""\
    logical_name,mode,prdSe,startPrdDe,endPrdDe,userStatsId,orgId,tblId,itmId,objL1,objL2,objL3,objL4,objL5,objL6,objL7,objL8,newEstPrdCnt,prdInterval,outputFields
    macro.cpi,param,M,200001,,,<ORGID>,<TBLID>,<ITMID>,<OBJL1>,,,,,,,,,,"PRD_DE,DT,UNIT_NM"
    asset.kospi,user,M,200001,,<USER_STATS_ID>,,,,,,,,,,,,,,"PRD_DE,DT,UNIT_NM"
    """)
    Path(path).write_text(tpl, encoding="utf-8")
    print(f"[template] wrote {path}. Fill in real codes and re-run.")

if __name__ == "__main__":
    ensure_env()

    # ① Catalog build (with auto-fallback + auto-discover)
    step1 = [
        PY, "run_build_catalog.py",
        "--vwcd", "MT_ZTITLE",
        "--roots", "A1", "A2",
        "--out", "series_catalog.csv",
        "--max-depth", "6",
        "--auto-fallback",
        "--auto-discover",
        "--discover-max-tries", "500",
        "--discover-time-budget", "90"
    ]
    rc = run(step1)

    if rc == 2:
        # 자동발견 실패 → 템플릿 생성 후 깔끔 종료
        write_catalog_template("series_catalog.csv")
        print("\n❗ 자동으로 parentListId를 찾지 못했습니다.")
        print("   1) series_catalog.csv 에 실제 KOSIS 코드(orgId/tblId/objL1/itmId 또는 userStatsId)를 채운 뒤")
        print("   2) 다시 run_all.py 를 실행하세요.")
        sys.exit(0)
    elif rc != 0:
        sys.exit(rc)

    # ②~⑦ 계속
    steps = [
        [PY, "run_fetch_data.py", "--catalog", "series_catalog.csv", "--out", "out_data.parquet"],
        [PY, "run_step2_prepare.py", "--raw", "out_data.parquet", "--wide-out", "out_wide.parquet"],
        [PY, "run_step3_discover.py", "--wide", "out_wide.parquet", "--out", "out_signals.csv", "--corr-strong", "0.45", "--corr-medium", "0.30"],
        [PY, "run_step4_causal.py", "--wide", "out_wide.parquet", "--outdir", "out_causal", "--prefer", "macro.policy_rate", "macro.cpi", "macro.gdp_growth", "--lp_shock", "macro.policy_rate", "--asset_prefix", "asset."],
        [PY, "run_step5_scenario.py", "--scenario", "scenarios/example_basic.yaml", "--irf", "out_causal/irf_svar.csv", "--fallback-irf", "out_causal/irf_var.csv", "--lp", "out_causal/lp_betas.csv", "--outdir", "out_scenario", "--summary-h", "4"],
        [PY, "run_step6_report.py", "--signals", "out_signals.csv", "--irf-svar", "out_causal/irf_svar.csv", "--irf-var", "out_causal/irf_var.csv", "--lp", "out_causal/lp_betas.csv", "--scenario-dir", "out_scenario", "--outdir", "out_report", "--h-pick", "4"]
    ]
    for s in steps:
        rc = run(s)
        if rc != 0:
            sys.exit(rc)

    print("\n✅ All steps finished. See: out_report/report.md (and .html)")
