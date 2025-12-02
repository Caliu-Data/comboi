from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import yaml

from comboi.logging import get_logger

logger = get_logger(__name__)


class DbtRunner:
    """Executes dbt models for transformations in the medallion architecture."""

    def __init__(self, dbt_project_path: Path):
        """
        Initialize the dbt runner.

        Args:
            dbt_project_path: Path to the dbt project directory
        """
        self.dbt_project_path = dbt_project_path
        self.dbt_project_yml = dbt_project_path / "dbt_project.yml"

        if not self.dbt_project_yml.exists():
            raise FileNotFoundError(
                f"dbt_project.yml not found at {self.dbt_project_yml}. "
                "Ensure the dbt project is properly configured."
            )

    def run_transformation(
        self,
        model_name: str,
        output_path: Path,
        vars_dict: Dict[str, Any] | None = None,
        target: str = "dev",
    ) -> Path:
        """
        Run a single dbt model and export results to parquet.

        Args:
            model_name: Name of the dbt model to run
            output_path: Path where the output parquet file should be written
            vars_dict: Variables to pass to dbt (e.g., base paths)
            target: dbt target profile to use

        Returns:
            Path to the output parquet file
        """
        logger.info("Running dbt model", model=model_name)

        # Create a temporary profiles directory
        with tempfile.TemporaryDirectory() as profiles_dir:
            # Generate profiles.yml
            self._generate_profiles_yml(Path(profiles_dir), target)

            # Prepare dbt command
            cmd = [
                "dbt",
                "run",
                "--profiles-dir",
                profiles_dir,
                "--project-dir",
                str(self.dbt_project_path),
                "--select",
                model_name,
                "--target",
                target,
            ]

            # Add variables if provided
            if vars_dict:
                vars_json = self._vars_to_json(vars_dict)
                cmd.extend(["--vars", vars_json])

            # Run dbt
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=str(self.dbt_project_path),
                )
                logger.debug("dbt output", stdout=result.stdout)
            except subprocess.CalledProcessError as e:
                logger.error("dbt failed", stderr=e.stderr, stdout=e.stdout)
                raise RuntimeError(f"dbt run failed for model {model_name}: {e.stderr}")

            # Export the model result to parquet
            self._export_to_parquet(model_name, output_path, profiles_dir, target)

            logger.info("dbt transformation completed", model=model_name)
            return output_path

    def run_transformations(
        self,
        stage: str,
        transformations: List[Dict[str, Any]],
        output_dir: Path,
        input_base_paths: Dict[str, str],
    ) -> List[Path]:
        """
        Run multiple dbt transformations and return output paths.

        Args:
            stage: Stage name (silver, gold)
            transformations: List of transformation configs
            output_dir: Directory to write output parquet files
            input_base_paths: Base paths for different stages (bronze, silver, etc.)

        Returns:
            List of output parquet file paths
        """
        output_paths: List[Path] = []

        # Prepare variables for dbt
        vars_dict = {
            "bronze_base_path": input_base_paths.get("bronze", "data/bronze"),
            "silver_base_path": input_base_paths.get("silver", "data/silver"),
            "stage": stage,
        }

        for trans_config in transformations:
            trans_name = trans_config["name"]
            model_name = trans_config.get("model", trans_name)

            logger.info("Processing dbt transformation", stage=stage, model=model_name)

            # Determine output path
            output_path = output_dir / f"{trans_name}.parquet"

            # Run the transformation
            result_path = self.run_transformation(
                model_name=model_name,
                output_path=output_path,
                vars_dict=vars_dict,
                target="dev",
            )
            output_paths.append(result_path)

        return output_paths

    def _generate_profiles_yml(self, profiles_dir: Path, target: str) -> None:
        """Generate profiles.yml for dbt execution."""
        profiles_config = {
            "comboi": {
                "target": target,
                "outputs": {
                    target: {
                        "type": "duckdb",
                        "path": ":memory:",
                        "extensions": ["parquet", "httpfs"],
                        "settings": {
                            "threads": 4,
                        },
                    }
                },
            }
        }

        # Add Azure storage connection string if available
        azure_conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if azure_conn:
            profiles_config["comboi"]["outputs"][target]["settings"][
                "azure_storage_connection_string"
            ] = azure_conn

        profiles_file = profiles_dir / "profiles.yml"
        with profiles_file.open("w") as f:
            yaml.dump(profiles_config, f)

        logger.debug("Generated profiles.yml", path=str(profiles_file))

    def _export_to_parquet(
        self, model_name: str, output_path: Path, profiles_dir: str, target: str
    ) -> None:
        """
        Export dbt model results to parquet file using DuckDB.

        Args:
            model_name: Name of the dbt model
            output_path: Path to write parquet file
            profiles_dir: Directory containing profiles.yml
            target: dbt target name
        """
        import duckdb

        # dbt materializes models in the target schema (default: memory database)
        # We need to query the materialized table and export to parquet

        # Read the dbt target configuration to understand where data is
        target_path = self.dbt_project_path / "target"

        # For in-memory duckdb, we need to re-run the SQL to get results
        # Alternative: configure dbt to materialize to a file-based duckdb database

        # For now, we'll use a simpler approach: read the compiled SQL and execute it
        compiled_sql_path = target_path / "compiled" / "comboi_dbt" / "models"

        # Find the compiled SQL file
        sql_file = None
        for root, dirs, files in os.walk(compiled_sql_path):
            for file in files:
                if file == f"{model_name}.sql":
                    sql_file = Path(root) / file
                    break
            if sql_file:
                break

        if not sql_file or not sql_file.exists():
            raise FileNotFoundError(
                f"Compiled SQL for model {model_name} not found in {compiled_sql_path}"
            )

        # Read and execute the compiled SQL
        with sql_file.open("r") as f:
            compiled_sql = f.read()

        # Create DuckDB connection and export
        con = duckdb.connect(":memory:")
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Execute the compiled SQL and export to parquet
            con.execute(
                f"COPY ({compiled_sql}) TO '{output_path.as_posix()}' (FORMAT PARQUET)"
            )
            logger.debug("Exported model to parquet", model=model_name, path=str(output_path))
        finally:
            con.close()

    def _vars_to_json(self, vars_dict: Dict[str, Any]) -> str:
        """Convert variables dict to JSON string for dbt --vars parameter."""
        return json.dumps(vars_dict)

    def run_tests(
        self,
        model_name: str | None = None,
        target: str = "dev",
    ) -> bool:
        """
        Run dbt tests for a model or all models.

        Args:
            model_name: Optional model name to test. If None, tests all models.
            target: dbt target profile to use

        Returns:
            True if tests pass, False otherwise
        """
        logger.info("Running dbt tests", model=model_name or "all")

        with tempfile.TemporaryDirectory() as profiles_dir:
            self._generate_profiles_yml(Path(profiles_dir), target)

            cmd = [
                "dbt",
                "test",
                "--profiles-dir",
                profiles_dir,
                "--project-dir",
                str(self.dbt_project_path),
                "--target",
                target,
            ]

            if model_name:
                cmd.extend(["--select", model_name])

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=str(self.dbt_project_path),
                )
                logger.info("dbt tests passed", model=model_name or "all")
                logger.debug("dbt test output", stdout=result.stdout)
                return True
            except subprocess.CalledProcessError as e:
                logger.error("dbt tests failed", stderr=e.stderr, stdout=e.stdout)
                return False
