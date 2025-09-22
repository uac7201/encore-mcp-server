# validator.py
from jsonschema import Draft202012Validator
from typing import Any, Dict, List

# ===== Defaults (TARGET_NAMESPACE uses '-' so it matches the DNS-label regex) =====
DEFAULT_POSTGRES_JOB_CONF: Dict[str, Any] = {
    "spark_namespace": "spark-operator",
    "APP_NAME": "encore-workflow-demo",
    "PATH": "/shared/encore/tmp/widgets",
    "executor_instances": 1,
    "POSTGRES_TABLE_NAME": "widgets",
    "TARGET_NAMESPACE": "spark-maik",  # '-' instead of '_' to satisfy DNS-label pattern
    "TARGET_TABLE": "maikspark_demo",
    "WRITE_MODE": "append",
}

# Schema for the new Airflow/Spark job config
# (strict: only the keys below are allowed; toggle additionalProperties to True if you want to allow extras)
POSTGRES_JOB_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "required": [
        "spark_namespace",
        "APP_NAME",
        "PATH",
        "executor_instances",
        "POSTGRES_TABLE_NAME",
        "TARGET_NAMESPACE",
        "TARGET_TABLE",
        "WRITE_MODE",
    ],
    "properties": {
        "spark_namespace": {
            "type": "string",
            # Kubernetes namespace DNS label
            "pattern": r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
            "minLength": 1,
            "maxLength": 63,
            "description": "Namespace where the Spark operator runs / submits jobs.",
            "default": DEFAULT_POSTGRES_JOB_CONF["spark_namespace"],
        },
        "APP_NAME": {
            "type": "string",
            "minLength": 1,
            # common app-name charset (feel free to loosen)
            "pattern": r"^[A-Za-z0-9._\-]+$",
            "description": "Spark application name.",
            "default": DEFAULT_POSTGRES_JOB_CONF["APP_NAME"],
        },
        "PATH": {
            "type": "string",
            "minLength": 1,
            # enforce absolute path (e.g., /shared/encore/tmp/widgets)
            "pattern": r"^/.*",
            "description": "Base path for inputs/outputs.",
            "default": DEFAULT_POSTGRES_JOB_CONF["PATH"],
        },
        "executor_instances": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1000,
            "description": "Number of Spark executor instances.",
            "default": DEFAULT_POSTGRES_JOB_CONF["executor_instances"],
        },
        "POSTGRES_TABLE_NAME": {
            "type": "string",
            "minLength": 1,
            "description": "Source table (e.g., widgets).",
            "default": DEFAULT_POSTGRES_JOB_CONF["POSTGRES_TABLE_NAME"],
        },
        "TARGET_NAMESPACE": {
            "type": "string",
            "pattern": r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
            "minLength": 1,
            "maxLength": 63,
            "description": "Kubernetes namespace where the job resources should live.",
            "default": DEFAULT_POSTGRES_JOB_CONF["TARGET_NAMESPACE"],
        },
        "TARGET_TABLE": {
            "type": "string",
            "minLength": 1,
            "description": "Destination table (e.g., maikspark_demo).",
            "default": DEFAULT_POSTGRES_JOB_CONF["TARGET_TABLE"],
        },
        "WRITE_MODE": {
            "type": "string",
            # common Spark write modes; you can trim this to just ["append"] if you want
            "enum": ["append", "overwrite", "ignore", "errorifexists"],
            "default": DEFAULT_POSTGRES_JOB_CONF["WRITE_MODE"],
            "description": "Write mode for the sink.",
        },
    },
    "additionalProperties": False,
}


def validate_conf(conf: Dict[str, Any], schema: Dict[str, Any] = POSTGRES_JOB_SCHEMA) -> None:
    """
    Validate a config dict against the schema. Raises ValueError with a tidy message if invalid.

    Note: jsonschema does not auto-apply defaults. See `apply_defaults_and_validate` below
    if you want to merge defaults before validating.
    """
    validator = Draft202012Validator(schema)
    errors: List[str] = []
    for e in sorted(validator.iter_errors(conf), key=lambda err: err.path):
        # build a readable "a/b/c: message"
        where = "/".join(str(p) for p in e.path) or "(root)"
        errors.append(f"- {where}: {e.message}")
    if errors:
        raise ValueError("Missing or invalid fields:\n" + "\n".join(errors))


def apply_defaults_and_validate(
    conf: Dict[str, Any],
    schema: Dict[str, Any] = POSTGRES_JOB_SCHEMA,
    defaults: Dict[str, Any] = DEFAULT_POSTGRES_JOB_CONF,
) -> Dict[str, Any]:
    """
    Merge defaults into `conf` (without mutating input), validate, and return the merged dict.
    This is handy if callers want missing keys to be filled from defaults.
    """
    merged = {**defaults, **conf}
    validate_conf(merged, schema)
    return merged
