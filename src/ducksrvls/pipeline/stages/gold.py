from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
from rich.console import Console

from ducksrvls.io.adls import ADLSClient

console = Console()


@dataclass
class GoldStage:
    data_lake: ADLSClient
    local_gold: Path

    def run(self, stage_conf: Dict) -> List[str]:
        outputs: List[str] = []
        for metric in stage_conf["metrics"]:
            console.log(f"[bold blue]Calculating metric {metric['name']}[/]")
            local_path = self.local_gold / f"{metric['name']}.parquet"
            local_path.parent.mkdir(parents=True, exist_ok=True)

            con = duckdb.connect()
            try:
                for ref in metric.get("silver_inputs", []):
                    con.execute(
                        f"CREATE OR REPLACE VIEW {ref['alias']} AS "
                        f"SELECT * FROM read_parquet('{ref['uri']}')"
                    )
                con.execute(
                    f"COPY ({metric['sql']}) TO '{local_path.as_posix()}' (FORMAT PARQUET)"
                )
            finally:
                con.close()

            remote_path = stage_conf["remote_path_template"].format(
                stage="gold",
                source="metrics",
                table=metric["name"],
            )
            remote_uri = self.data_lake.upload(local_path, remote_path)
            outputs.append(remote_uri)

        console.log(f"[bold green]Gold stage produced {len(outputs)} metrics[/]")
        return outputs

