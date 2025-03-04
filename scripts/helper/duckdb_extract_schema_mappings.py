import os
import json
from public_bi import *

# Mapping PUBLIC_BI types to DuckDB types based on C++ TypeLookUp function
PUBLIC_BI_TYPES = {
    # FastLanes types
    "FLS_I64": "BIGINT",
    "FLS_I32": "INTEGER",
    "FLS_I08": "TINYINT",
    "FLS_U08": "UTINYINT",
    "FLS_DBL": "DOUBLE",
    "FLS_STR": "VARCHAR",

    # General SQL types
    "BIGINT": "BIGINT",
    "string": "VARCHAR",
    "varchar": "VARCHAR",
    "VARCHAR": "VARCHAR",
    "double": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "list": "LIST",
    "struct": "STRUCT",
    "map": "MAP",

    # ClickHouse mappings
    "SMALLINT": "SMALLINT",
    "INTEGER": "INTEGER",
    "VARCHAR(255)": "VARCHAR",
    "CHAR": "VARCHAR",

    # PublicBI mappings
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "integer": "INTEGER",
    "smallint": "SMALLINT",

    # Other types
    "time": "TIME",
    "timestamp": "TIMESTAMP",
}

# Add decimal(1,0) to decimal(18,17) dynamically
PUBLIC_BI_TYPES.update({f"decimal({p}, {s})": f"DECIMAL({p}, {s})" for p in range(1, 19) for s in range(0, p + 1)})

# Add varchar(1) to varchar(8160) dynamically
PUBLIC_BI_TYPES.update({f"varchar({i})": "VARCHAR" for i in range(1, 8161)})

SCHEMA_MAPPING_PATH = "schema_mappings.json"  # Ensure this is a valid path


def extract_and_store_schema_mappings():
    """
    Extracts column type mappings from schema files and stores them in a JSON file.
    Throws an exception if an unknown type is encountered.
    """
    schema_mappings = {}

    for table in PublicBI.table_list:
        schema_path = PublicBI.get_schema_file_path(table)

        # Read schema JSON file
        with open(schema_path, "r") as schema_file:
            schema_json = json.load(schema_file)

        column_types = {}
        for col in schema_json["columns"]:
            col_type = col["type"]
            if col_type not in PUBLIC_BI_TYPES:
                raise ValueError(f"Unknown column type '{col_type}' in table '{table}'. Please update PUBLIC_BI_TYPES.")
            column_types[col["name"]] = PUBLIC_BI_TYPES[col_type]

        schema_mappings[table] = column_types

    # Ensure the directory exists before writing the JSON file
    schema_dir = os.path.dirname(SCHEMA_MAPPING_PATH)
    if schema_dir:  # Avoid trying to create an empty directory
        os.makedirs(schema_dir, exist_ok=True)

    with open(SCHEMA_MAPPING_PATH, "w") as json_file:
        json.dump(schema_mappings, json_file, indent=4)

    print(f"Schema mappings saved to {SCHEMA_MAPPING_PATH}")


if __name__ == "__main__":
    extract_and_store_schema_mappings()
