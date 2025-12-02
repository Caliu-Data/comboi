from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
from splink.duckdb.duckdb_linker import DuckDBLinker

from comboi.bruin_runner import BruinRunner
from comboi.bruin_quality import BruinQualityRunner
from comboi.dbt_runner import DbtRunner
from comboi.io.adls import ADLSClient
from comboi.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SilverStage:
    data_lake: ADLSClient
    local_silver: Path

    def run(self, stage_conf: Dict) -> List[str]:
        outputs: List[str] = []

        # Get configuration
        transformations_path = Path(stage_conf.get("transformations_path", "transformations"))
        contracts_path = Path(stage_conf.get("contracts_path", "contracts"))
        dbt_project_path = Path(stage_conf.get("dbt_project_path", "dbt_project"))
        bronze_base_path = stage_conf.get("bronze_base_path", "data/bronze")

        # Get transformations config
        transformations = stage_conf.get("transformations", {})
        silver_transforms = transformations.get("silver", [])

        if not silver_transforms:
            logger.warning("No transformations configured for Silver stage")
            return outputs

        # Initialize runners
        bruin_runner = BruinRunner(transformations_path=transformations_path)
        dbt_runner = None
        if dbt_project_path.exists():
            dbt_runner = DbtRunner(dbt_project_path=dbt_project_path)

        # Separate transformations by type
        bruin_transforms = [t for t in silver_transforms if t.get("type", "bruin") == "bruin"]
        dbt_transforms = [t for t in silver_transforms if t.get("type", "bruin") == "dbt"]

        # Run bruin transformations
        input_base_paths = {"bronze": bronze_base_path}
        bruin_outputs = []
        if bruin_transforms:
            bruin_outputs = bruin_runner.run_transformations(
                "silver",
                bruin_transforms,
                self.local_silver,
                input_base_paths,
            )

        # Run dbt transformations
        dbt_outputs = []
        if dbt_transforms:
            if not dbt_runner:
                raise RuntimeError(
                    f"dbt transformations configured but dbt project not found at {dbt_project_path}"
                )
            dbt_outputs = dbt_runner.run_transformations(
                "silver",
                dbt_transforms,
                self.local_silver,
                input_base_paths,
            )

        # Combine outputs and process each transformation
        all_transforms = bruin_transforms + dbt_transforms
        all_outputs = bruin_outputs + dbt_outputs

        # Process each transformation output
        for trans_config, output_path in zip(all_transforms, all_outputs):
            trans_name = trans_config["name"]
            logger.info("Processing Silver transformation", transformation=trans_name)

            # Run quality checks (supports contracts via "contract:" prefix)
            # Quality checks work for both bruin and dbt transformations
            if "quality_checks" in trans_config:
                quality_runner = BruinQualityRunner(
                    transformations_path=transformations_path,
                    contracts_path=contracts_path,
                )
                quality_runner.run_quality_checks(
                    trans_config["quality_checks"],
                    output_path,
                    trans_name,
                )

            # Run Splink deduplication if configured
            # Works for both bruin and dbt transformations
            if "splink" in trans_config:
                self._run_splink(trans_config, output_path)

            # Upload to ADLS
            remote_path = stage_conf["remote_path_template"].format(
                stage="silver", source="refined", table=trans_name
            )
            remote_uri = self.data_lake.upload(output_path, remote_path)
            outputs.append(remote_uri)

        logger.info("Silver stage completed", datasets_produced=len(outputs))
        return outputs

    def _run_splink(self, trans_config: Dict, local_path: Path) -> None:
        splink_cfg = trans_config.get("splink")
        if not splink_cfg:
            return
        trans_name = trans_config["name"]
        logger.info("Running Splink deduplication", transformation=trans_name)
        linker = DuckDBLinker(
            input_table_or_tables=[
                {
                    "table_name": trans_name,
                    "sql": f"SELECT * FROM read_parquet('{local_path.as_posix()}')",
                }
            ],
            settings_dict=splink_cfg,
        )
        splink_df = linker.deduplicate_table(
            trans_name,
            blocking_rule=splink_cfg.get("blocking_rule"),
            retain_matching_columns=True,
        )
        tmp_path = local_path.with_suffix(".dedup.parquet")
        linker.duckdb_connection().execute(
            f"COPY (SELECT * FROM {splink_df.physical_name}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET)"
        )
        tmp_path.replace(local_path)

