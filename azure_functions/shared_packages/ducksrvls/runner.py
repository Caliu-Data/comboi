from __future__ import annotations

from pathlib import Path

from ducksrvls.checkpoint import CheckpointStore
from ducksrvls.config import load_config
from ducksrvls.pipeline.driver import Driver


def create_driver(config_path: Path) -> Driver:
    resolved_config = _resolve_path(config_path)
    config = load_config(resolved_config)
    _normalize_paths(config, resolved_config.parent)
    checkpoint_path = Path(config.stages.bronze["checkpoint_path"])
    checkpoint_store = CheckpointStore(_resolve_relative(checkpoint_path, resolved_config.parent))
    return Driver(config=config, checkpoint_store=checkpoint_store)


def _resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    root = Path(__file__).resolve().parents[2]
    candidate = (root / path).resolve()
    if candidate.exists():
        return candidate
    return (Path.cwd() / path).resolve()


def _resolve_relative(path: Path, base: Path) -> Path:
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _normalize_paths(config, base: Path) -> None:
    config.monitoring.log_path = _resolve_relative(Path(config.monitoring.log_path), base)
    config.monitoring.metrics_path = _resolve_relative(Path(config.monitoring.metrics_path), base)

    if config.monitoring.azure_connection_string:
        pass

    bronze = config.stages.bronze
    bronze["local_path"] = str(_resolve_relative(Path(bronze["local_path"]), base))
    if "checkpoint_path" in bronze:
        bronze["checkpoint_path"] = str(_resolve_relative(Path(bronze["checkpoint_path"]), base))

    silver = config.stages.silver
    silver["local_path"] = str(_resolve_relative(Path(silver["local_path"]), base))

    gold = config.stages.gold
    gold["local_path"] = str(_resolve_relative(Path(gold["local_path"]), base))

