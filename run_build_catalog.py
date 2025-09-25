from __future__ import annotations
import os, argparse


def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        choices=["userstats", "direct", "discover"],
        default=os.getenv("KOSIS_MODE", "userstats"),
    )
    p.add_argument("--vwcd", default=os.getenv("KOSIS_VWCD", "MT_ZTITLE"))
    p.add_argument("--roots", nargs="*", default=[os.getenv("KOSIS_ROOTS", "AUTO")])
    p.add_argument("--out", default=os.getenv("KOSIS_OUT", "series_catalog.csv"))
    p.add_argument("--max-depth", type=int, default=int(os.getenv("KOSIS_MAX_DEPTH", "5")))
    p.add_argument("--leaf-cap", type=int, default=int(os.getenv("KOSIS_LEAF_CAP", "5000")))
    p.add_argument("--probe", action="store_true")
    p.add_argument("--verbose", action="store_true")
    # userstats 전용
    p.add_argument("--userstats", nargs="*", default=None)
    p.add_argument("--prdSe", default=os.getenv("KOSIS_PRDSE", None))
    p.add_argument("--start", dest="startPrdDe", default=os.getenv("KOSIS_START", None))
    p.add_argument("--end", dest="endPrdDe", default=os.getenv("KOSIS_END", None))
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.mode == "userstats":
        from src.userstats_runner import run_userstats_batch

        if args.verbose:
            print("[build] mode=userstats")
        return run_userstats_batch(
            args.userstats,
            out=args.out,
            verbose=args.verbose,
            prdSe=args.prdSe,
            startPrdDe=args.startPrdDe,
            endPrdDe=args.endPrdDe,
        )
    if args.mode == "direct":
        from src.direct_catalog import run_direct_catalog

        if args.verbose:
            print("[build] mode=direct")
        return run_direct_catalog(
            args.vwcd,
            args.roots,
            args.out,
            max_depth=args.max_depth,
            verbose=args.verbose,
        )
    from src.catalog_builder import build_catalog

    if args.verbose:
        print("[build] mode=discover")
    build_catalog(
        args.vwcd,
        args.roots,
        out=args.out,
        max_depth=args.max_depth,
        verbose=args.verbose,
        probe=args.probe,
        leaf_cap=args.leaf_cap,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

