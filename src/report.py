from datetime import datetime

def render_report(path, macro_bullets, invest_df):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Macro→Investment Report\nGenerated: {datetime.now().isoformat()}\n\n")
        f.write("## Macro Conclusions\n")
        for ln in macro_bullets:
            f.write(f"- {ln}\n")
        f.write("\n## Investment Outlook\n| Asset | Score | Stance |\n|---|---:|---|\n")
        for _,r in invest_df.iterrows():
            f.write(f"| {r.asset} | {r.score:.3f} | {r.stance} |\n")
