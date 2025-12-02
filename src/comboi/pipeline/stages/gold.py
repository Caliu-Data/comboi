from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from comboi.bruin_runner import BruinRunner
from comboi.dbt_runner import DbtRunner
from comboi.io.adls import ADLSClient
from comboi.logging import get_logger

logger = get_logger(__name__)


@dataclass
class GoldStage:
    data_lake: ADLSClient
    local_gold: Path

    def run(self, stage_conf: Dict) -> List[str]:
        outputs: List[str] = []

        # Get configuration
        transformations_path = Path(stage_conf.get("transformations_path", "transformations"))
        dbt_project_path = Path(stage_conf.get("dbt_project_path", "dbt_project"))
        silver_base_path = stage_conf.get("silver_base_path", "data/silver")

        # Get transformations config
        transformations = stage_conf.get("transformations", {})
        gold_transforms = transformations.get("gold", [])

        if not gold_transforms:
            logger.warning("No transformations configured for Gold stage")
            return outputs

        # Initialize runners
        bruin_runner = BruinRunner(transformations_path=transformations_path)
        dbt_runner = None
        if dbt_project_path.exists():
            dbt_runner = DbtRunner(dbt_project_path=dbt_project_path)

        # Separate transformations by type
        bruin_transforms = [t for t in gold_transforms if t.get("type", "bruin") == "bruin"]
        dbt_transforms = [t for t in gold_transforms if t.get("type", "bruin") == "dbt"]

        # Run bruin transformations
        input_base_paths = {"silver": silver_base_path}
        bruin_outputs = []
        if bruin_transforms:
            bruin_outputs = bruin_runner.run_transformations(
                "gold",
                bruin_transforms,
                self.local_gold,
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
                "gold",
                dbt_transforms,
                self.local_gold,
                input_base_paths,
            )

        # Combine outputs and upload each transformation to ADLS
        all_transforms = bruin_transforms + dbt_transforms
        all_outputs = bruin_outputs + dbt_outputs

        for trans_config, output_path in zip(all_transforms, all_outputs):
            trans_name = trans_config["name"]
            logger.info("Uploading Gold transformation", transformation=trans_name)

            remote_path = stage_conf["remote_path_template"].format(
                stage="gold",
                source="metrics",
                table=trans_name,
            )
            remote_uri = self.data_lake.upload(output_path, remote_path)
            outputs.append(remote_uri)

        logger.info("Gold stage completed", metrics_produced=len(outputs))
        return outputs

