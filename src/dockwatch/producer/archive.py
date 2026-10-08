"""Raw archive of every fetched snapshot (gzip JSON), the replay source for throughput tests and reprocessing."""

import gzip
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol


def archive_key(feed: str, last_updated: int) -> str:
    ts = datetime.fromtimestamp(last_updated, tz=UTC)
    return f"raw/gbfs/{feed}/dt={ts:%Y-%m-%d}/{ts:%H}/{last_updated}.json.gz"


class Archive(Protocol):
    def put(self, key: str, raw: bytes) -> None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def get(self, key: str) -> bytes: ...


class FsArchive:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def put(self, key: str, raw: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(raw))

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*.json.gz"))

    def get(self, key: str) -> bytes:
        return gzip.decompress((self.root / key).read_bytes())


class S3Archive:
    def __init__(self, bucket: str, client) -> None:
        self.bucket = bucket
        self.s3 = client

    def put(self, key: str, raw: bytes) -> None:
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=gzip.compress(raw),
            ContentType="application/json",
            ContentEncoding="gzip",
        )

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def get(self, key: str) -> bytes:
        return gzip.decompress(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())

    def ensure_bucket(self) -> bool:
        """Create the bucket if missing (local S3 only; on AWS Terraform owns it). Returns True if created."""
        existing = {b["Name"] for b in self.s3.list_buckets().get("Buckets", [])}
        if self.bucket in existing:
            return False
        self.s3.create_bucket(Bucket=self.bucket)
        return True


def build_archive(settings) -> Archive:
    if settings.archive_kind == "fs":
        return FsArchive(settings.archive_fs_root)
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint if settings.target == "local" else None,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )
    return S3Archive(settings.s3_bucket, client)
