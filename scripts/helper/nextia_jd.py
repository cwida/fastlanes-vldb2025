# scripts/helper/nextia_jd.py

import os

# ──────────────────────────────────────────────────────────────────────────────
# Compute your project’s root & data_repo paths
# ──────────────────────────────────────────────────────────────────────────────
_THIS_DIR = os.path.dirname(__file__)  # .../scripts/helper
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
_DATA_REPO_DIR = os.path.join(_PROJECT_ROOT, "data_repo")

# ──────────────────────────────────────────────────────────────────────────────
# NextiaJD tables folder & static list
# ──────────────────────────────────────────────────────────────────────────────
_NAME = "NextiaJD"
_TABLES_DIR = os.path.join(_DATA_REPO_DIR, _NAME, "tables")

# discover all subdirectories under data_repo/NextiaJD/tables
_TABLES = sorted(
    d for d in os.listdir(_TABLES_DIR)
    if os.path.isdir(os.path.join(_TABLES_DIR, d))
)


class NextiaJD:
    """
    Helper for the NextiaJD dataset.
    Mirrors public_bi.PublicBI’s interface, but
    points at data_repo/NextiaJD/tables/<table_name>/{.csv, schema.json, schema.yaml}.
    """
    name = _NAME
    _tables_dir = _TABLES_DIR
    table_list = _TABLES

    @staticmethod
    def get_dir_path(table_name: str) -> str:
        return os.path.join(NextiaJD._tables_dir, table_name)

    @staticmethod
    def get_file_path(table_name: str) -> str:
        return os.path.join(
            NextiaJD.get_dir_path(table_name),
            f"{table_name}.csv"
        )

    @staticmethod
    def get_schema_file_path(table_name: str) -> str:
        return os.path.join(
            NextiaJD.get_dir_path(table_name),
            "schema.json"
        )

    @staticmethod
    def get_yaml_schema_file_path(table_name: str) -> str:
        return os.path.join(
            NextiaJD.get_dir_path(table_name),
            "schema.yaml"
        )

    @staticmethod
    def is_valid_file(path: str) -> bool:
        return os.path.exists(path) and os.path.getsize(path) > 0

    @staticmethod
    def get_dataset_list():
        return iter(NextiaJD.table_list)

    @staticmethod
    def get_n_table() -> int:
        return len(NextiaJD.table_list)
