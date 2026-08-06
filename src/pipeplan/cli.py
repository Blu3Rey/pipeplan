"""Command-line interface.

Subcommands:

* ``validate`` -- assemble and statically validate a blueprint, print the
  resolved execution order, and exit. Touches no data.
* ``run``      -- validate then execute the pipeline.
* ``schema``   -- emit the JSON Schema of the blueprint format (for editors).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from .config.loader import load_config
from .config.models import PipelineConfig
from .core.exceptions import PipePlanError
from .orchestrator import Orchestrator


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeplan",
        description="Declarative, configuration-driven batch ETL on pandas.",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase log verbosity (-v info, -vv debug).")
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd, help_text in (("validate", "validate a blueprint without running it."),
                           ("run", "validate and execute a pipeline.")):
        p = sub.add_parser(cmd, help=help_text)
        p.add_argument("blueprint", help="path to the root blueprint (YAML or JSON).")
        p.add_argument("--param", action="append", default=[], metavar="KEY=VALUE",
                       help="set a run parameter (repeatable).")
        p.add_argument("--strict", action="store_true",
                       help="fail if any ${...} token cannot be resolved.")

    sub.add_parser("schema", help="print the blueprint JSON Schema and exit.")
    return parser


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _parse_params(items: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise PipePlanError(f"--param expects KEY=VALUE, got '{item}'")
        key, _, value = item.partition("=")
        params[key.strip()] = value
    return params


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(getattr(args, "verbose", 0))

    try:
        if args.command == "schema":
            print(json.dumps(PipelineConfig.model_json_schema(), indent=2))
            return 0

        params = _parse_params(args.param)
        config = load_config(args.blueprint, params=params, strict=args.strict)
        orchestrator = Orchestrator(config)

        if args.command == "validate":
            order = orchestrator.execution_order()
            print(f"blueprint OK: pipeline '{config.pipeline_id}' ({config.apiVersion})")
            print(f"resources: {', '.join(config.resources) or '<none>'}")
            print(f"schemas:   {', '.join(config.schemas) or '<none>'}")
            print(f"execution order: {' -> '.join(order)}")
            return 0

        if args.command == "run":
            ctx = orchestrator.run()
            print(f"pipeline '{config.pipeline_id}' completed.")
            print(f"materialised dataframes: {', '.join(ctx.state.names()) or '<none>'}")
            return 0
    except PipePlanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0  # pragma: no cover - argparse guarantees a command


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
