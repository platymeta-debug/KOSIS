"""Validation helpers for KOSIS period parameters."""

from __future__ import annotations

import re
from typing import Tuple

# prdSe 허용: Y(연), M(월), Q(분기), S(반기), D(일), F(수시), IR(부정기) 등
VALID_PRDSE = {"Y", "M", "Q", "S", "D", "F", "IR"}


def validate_prdse(prd_se: str) -> None:
    """Ensure the provided ``prdSe`` value matches the official whitelist."""

    if prd_se not in VALID_PRDSE:
        raise ValueError(
            f"prdSe가 올바르지 않습니다: {prd_se} (허용: {sorted(VALID_PRDSE)})"
        )


def normalize_period_str(prd_se: str, yyyx: str) -> str:
    """Normalize the period string according to the KOSIS specification."""

    s = re.sub(r"[^0-9Qq]", "", str(yyyx))
    if prd_se == "Q":
        match = re.match(r"^(\d{4})[Qq]([1-4])$", s)
        if not match:
            raise ValueError(
                f"분기 prdSe=Q는 YYYYQ[1-4] 형식이어야 합니다: {yyyx}"
            )
        return f"{match.group(1)}Q{match.group(2)}"
    if prd_se == "M":
        match = re.match(r"^(\d{4})(0[1-9]|1[0-2])$", s)
        if not match:
            raise ValueError(f"월 prdSe=M은 YYYYMM 형식이어야 합니다: {yyyx}")
        return f"{match.group(1)}{match.group(2)}"
    if prd_se == "Y":
        match = re.match(r"^(\d{4})$", s)
        if not match:
            raise ValueError(f"연 prdSe=Y는 YYYY 형식이어야 합니다: {yyyx}")
        return match.group(1)
    # S/D/F/IR 등은 데이터셋별 케이스가 다양 → 우선 원본 유지(필요 시 확장)
    return yyyx


def normalize_range(
    prd_se: str, start: str | None = None, end: str | None = None
) -> Tuple[str | None, str | None]:
    """Normalize start/end period strings using the validation helpers."""

    validate_prdse(prd_se)
    ns = normalize_period_str(prd_se, start) if start else None
    ne = normalize_period_str(prd_se, end) if end else None
    return ns, ne
