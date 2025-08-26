from jsonschema import Draft202012Validator, ValidationError
from typing import Any, Dict
POSTGRES_JOB_SCHEMA = {
    "type": "object",
    "required": [
        "JDBC_URL",
        "JDBC_USERNAME",
        "JDBC_PASSWORD",
        "JDBC_DRIVER",
        "JDBC_TABLE",
        "JDBC_QUERY",
        "APP_NAME"
    ],
    "properties": {
        "JDBC_URL": {
            "type": "string",
            "description": "Full JDBC URL, e.g. jdbc:postgresql://host:5432/db"
        },
        "JDBC_USERNAME": {"type": "string"},
        "JDBC_PASSWORD": {"type": "string"},
        "JDBC_DRIVER": {"type": "string", "default": "org.postgresql.Driver"},
        "JDBC_TABLE": {"type": "string", "description": "e.g. public.orders"},
        "JDBC_QUERY": {"type": "string", "description": "SELECT statement"},
        "APP_NAME": {"type": "string", "description": "Spark job name"}
    }
}

def validate_conf(conf: Dict[str, Any], schema: Dict[str, Any]) -> None:
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(conf), key=lambda e: e.path)
    if errors:
        message = "\n".join(
            f"- {'/'.join(str(p) for p in e.path)}: {e.message}" for e in errors
        )
        raise ValueError(f"Missing or invalid fields:\n{message}")
