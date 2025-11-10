from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
from rich.console import Console
from soda.scan import Scan
from splink.duckdb.duckdb_linker import DuckDBLinker

from ducksrvls.io.adls import ADLSClient

console = Console()


@dataclass
class SilverStage:
    data_lake: ADLSClient
    local_silver: Path

    def run(self, stage_conf: Dict) -> List[str]:
        outputs: List[str] = []
        for dataset in stage_conf["datasets"]:
            bronze_uri = dataset["bronze_uri"]
            local_path = self.local_silver / f"{dataset['name']}.parquet"
            local_path.parent.mkdir(parents=True, exist_ok=True)

            console.log(f"[bold blue]Transforming bronze dataset {dataset['name']}[/]")
            con = duckdb.connect()
            try:
                con.execute(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{bronze_uri}')")
                for statement in dataset.get("transformations", []):
                    con.execute(statement)
                dedup_view = dataset.get("deduplication_view", "bronze_clean")
                con.execute(
                    f"COPY (SELECT * FROM {dedup_view}) TO '{local_path.as_posix()}' (FORMAT PARQUET)"
                )
            finally:
                con.close()

            self._run_soda(dataset)
            self._run_splink(dataset, local_path)

            remote_path = stage_conf["remote_path_template"].format(
                stage="silver", source="refined", table=dataset["name"]
            )
            remote_uri = self.data_lake.upload(local_path, remote_path)
            outputs.append(remote_uri)

        console.log(f"[bold green]Silver stage produced {len(outputs)} datasets[/]")
        return outputs

    def _run_soda(self, dataset: Dict) -> None:
        soda_cfg = dataset.get("soda")
        if not soda_cfg:
            return
        console.log(f"[cyan]Running Soda scan for {dataset['name']}[/]")
        scan = Scan()
        if configuration := soda_cfg.get("config"):
            scan.add_configuration_yaml_file(configuration)
        if checks := soda_cfg.get("checks"):
            scan.add_sodacl_yaml_file(checks)
        scan.set_scan_definition_name(dataset["name"])
        scan.execute()
        if scan.has_check_failures() or scan.has_error_logs():
            raise RuntimeError(f"Soda checks failed for {dataset['name']}")

    def _run_splink(self, dataset: Dict, local_path: Path) -> None:
        splink_cfg = dataset.get("splink")
        if not splink_cfg:
            return
        console.log(f"[cyan]Running Splink deduplication for {dataset['name']}[/]")
        linker = DuckDBLinker(
            input_table_or_tables=[
                {
                    "table_name": dataset["name"],
                    "sql": f"SELECT * FROM read_parquet('{local_path.as_posix()}')",
                }
            ],
            settings_dict=splink_cfg["settings"],
        )
        splink_df = linker.deduplicate_table(
            dataset["name"],
            blocking_rule=splink_cfg.get("blocking_rule"),
            retain_matching_columns=True,
        )
        tmp_path = local_path.with_suffix(".dedup.parquet")
        linker.duckdb_connection().execute(
            f"COPY (SELECT * FROM {splink_df.physical_name}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET)"
        )
        tmp_path.replace(local_path)

