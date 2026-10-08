"""Run one Spark SQL query against the lake and print it. Run: python tasks.py sql "SELECT ... FROM lake.silver..." """

import sys

from dockwatch.config import get_settings
from dockwatch.streaming.lake import build_session


def main() -> None:
    spark = build_session(get_settings(), "dockwatch-sql")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(" ".join(sys.argv[1:])).show(50, truncate=False)


if __name__ == "__main__":
    main()
