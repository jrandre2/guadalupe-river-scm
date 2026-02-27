"""Acquisition pipeline orchestrator.

Runs data acquisition modules in dependency order. Each module is expected
to expose a `run()` function that downloads and saves its data.
"""

from __future__ import annotations

import importlib
import sys
import traceback
from typing import Any

import click

from src.utils.logging_setup import get_logger, setup_logging

log = get_logger(__name__)

# Task DAG: source_name -> {depends_on, phase, module_path}
TASKS: dict[str, dict[str, Any]] = {
    # Phase 1: Anchor datasets + donor pool
    "fema_declarations": {"depends_on": [], "phase": 1, "module": "src.acquire.fema_declarations"},
    "donor_pool":        {"depends_on": ["fema_declarations"], "phase": 1, "module": "src.acquire.donor_pool"},
    "bea_income":        {"depends_on": [], "phase": 1, "module": "src.acquire.bea_income"},
    "census_bds":        {"depends_on": [], "phase": 1, "module": "src.acquire.census_bds"},
    "bls_qcew":          {"depends_on": [], "phase": 1, "module": "src.acquire.bls_qcew"},
    # Phase 2: Extended indicators + funding
    "bls_laus":          {"depends_on": [], "phase": 2, "module": "src.acquire.bls_laus"},
    "census_cbp":        {"depends_on": [], "phase": 2, "module": "src.acquire.census_cbp"},
    "census_bps":        {"depends_on": [], "phase": 2, "module": "src.acquire.census_bps"},
    "irs_soi":           {"depends_on": [], "phase": 2, "module": "src.acquire.irs_soi"},
    "census_qwi":        {"depends_on": [], "phase": 2, "module": "src.acquire.census_qwi"},
    "census_acs":        {"depends_on": [], "phase": 2, "module": "src.acquire.census_acs"},
    "fema_pa":           {"depends_on": ["fema_declarations"], "phase": 2, "module": "src.acquire.fema_pa"},
    "fema_ia":           {"depends_on": ["fema_declarations"], "phase": 2, "module": "src.acquire.fema_ia"},
    "fema_hma":          {"depends_on": ["fema_declarations"], "phase": 2, "module": "src.acquire.fema_hma"},
    "fema_nfip":         {"depends_on": [], "phase": 2, "module": "src.acquire.fema_nfip"},
    "sba_loans":         {"depends_on": [], "phase": 2, "module": "src.acquire.sba_loans"},
    "noaa_storms":       {"depends_on": [], "phase": 2, "module": "src.acquire.noaa_storms"},
    "usgs_nwis":         {"depends_on": [], "phase": 2, "module": "src.acquire.usgs_nwis"},
    # Phase 3: Manual/semi-auto
    "tx_comptroller":    {"depends_on": [], "phase": 3, "module": "src.acquire.tx_comptroller"},
    "hud_cdbgdr":        {"depends_on": [], "phase": 3, "module": "src.acquire.hud_cdbgdr"},
    "usaspending":       {"depends_on": ["fema_declarations"], "phase": 3, "module": "src.acquire.usaspending"},
}


def _topo_sort(tasks: dict[str, dict], phase_filter: int | None = None) -> list[str]:
    """Topological sort of task DAG, optionally filtering by phase."""
    filtered = {
        name: info for name, info in tasks.items()
        if phase_filter is None or info["phase"] <= phase_filter
    }

    # Kahn's algorithm
    in_degree: dict[str, int] = {name: 0 for name in filtered}
    for name, info in filtered.items():
        for dep in info["depends_on"]:
            if dep in filtered:
                in_degree[name] += 1

    queue = sorted(n for n, d in in_degree.items() if d == 0)
    order: list[str] = []

    while queue:
        node = queue.pop(0)
        order.append(node)
        for name, info in filtered.items():
            if node in info["depends_on"]:
                in_degree[name] -= 1
                if in_degree[name] == 0:
                    queue.append(name)
        queue.sort()

    return order


def run_task(name: str, force: bool = False) -> bool:
    """Run a single acquisition task by name. Returns True on success."""
    task_info = TASKS.get(name)
    if not task_info:
        log.error("unknown_task", name=name)
        return False

    log.info("task_start", name=name, phase=task_info["phase"])
    try:
        module = importlib.import_module(task_info["module"])
        module.run(force=force)
        log.info("task_complete", name=name)
        return True
    except Exception:
        log.error("task_failed", name=name, traceback=traceback.format_exc())
        return False


def run_pipeline(phase: int | None = None, tasks: list[str] | None = None, force: bool = False) -> None:
    """Run acquisition tasks in dependency order.

    Args:
        phase: Run tasks up to and including this phase (1, 2, or 3).
        tasks: Run only these specific tasks (overrides phase).
        force: Re-download even if output already exists.
    """
    if tasks:
        order = []
        # Resolve dependencies for requested tasks
        needed: set[str] = set()
        for t in tasks:
            needed.add(t)
            for dep in TASKS.get(t, {}).get("depends_on", []):
                needed.add(dep)
        order = _topo_sort({k: v for k, v in TASKS.items() if k in needed})
    else:
        order = _topo_sort(TASKS, phase_filter=phase)

    log.info("pipeline_start", n_tasks=len(order), task_order=order)

    results: dict[str, bool] = {}
    for name in order:
        deps = TASKS[name]["depends_on"]
        deps_ok = all(results.get(d, False) for d in deps if d in results)
        if not deps_ok:
            log.warning("task_skipped_dep_failed", name=name, deps=deps)
            results[name] = False
            continue
        results[name] = run_task(name, force=force)

    succeeded = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    log.info("pipeline_complete", succeeded=succeeded, failed=failed)


@click.command()
@click.option("--phase", type=int, default=None, help="Run phases up to N (1, 2, or 3)")
@click.option("--task", "tasks", multiple=True, help="Run specific task(s) by name")
@click.option("--force", is_flag=True, help="Re-download even if output exists")
@click.option("--list-tasks", is_flag=True, help="List all available tasks")
@click.option("--log-level", default="INFO", help="Logging level")
def cli(phase, tasks, force, list_tasks, log_level):
    """Guadalupe SCM data acquisition pipeline."""
    setup_logging(log_level)

    if list_tasks:
        for name, info in sorted(TASKS.items(), key=lambda x: (x[1]["phase"], x[0])):
            click.echo(f"  Phase {info['phase']}: {name}")
        return

    run_pipeline(
        phase=phase,
        tasks=list(tasks) if tasks else None,
        force=force,
    )


if __name__ == "__main__":
    cli()
