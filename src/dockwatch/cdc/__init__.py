"""CDC from the ops database into Iceberg (M3).

checksum.py is shared by the host (Postgres rows) and the Spark image (Iceberg rows), so everything in this package
stays Python 3.10-compatible (the Spark image's Python).
"""
