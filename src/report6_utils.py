from __future__ import annotations


def fmt_float(x, nd: int = 3) -> str:
    """Format numeric values safely for markdown/HTML output."""
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "-"
