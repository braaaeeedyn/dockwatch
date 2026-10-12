"""Airflow logging config for the DockWatch containers (AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS).

Airflow's default config, except that the `alembic` loggers only log warnings: alembic's plugin-setup INFO lines are
otherwise printed to stdout by every Airflow CLI call, ahead of `-o json` output.
"""

from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["loggers"]["alembic"] = {"handlers": ["console"], "level": "WARNING", "propagate": False}
