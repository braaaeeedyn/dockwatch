"""Alert notifications: turn gbfs.alerts records into a short email via SNS (AWS Lambda), or print them locally.

The Lambda is invoked with {"alerts": [<gbfs.alerts value>, ...], "stations": {station_id: name}} by a small
consumer (wired up in M6). Locally, `python -m dockwatch.alerts` would do the same without AWS.
"""

import json
import os
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")
KIND_WORDS = {"empty": "has no bikes", "full": "has no free docks"}


def format_alert(alert: dict, station_name: str | None) -> tuple[str, str]:
    """Return (subject, body). Plain English, Pacific time (DESIGN.md §10)."""
    name = station_name or alert["station_id"]
    minutes = round(alert["duration_s"] / 60)
    started = datetime.fromtimestamp(alert["start_ts"], tz=UTC).astimezone(PACIFIC).strftime("%I:%M %p").lstrip("0")
    if alert["action"] == "raised":
        subject = f"DockWatch: {name} {KIND_WORDS[alert['kind']]} ({minutes} min)"
        body = f"{name} {KIND_WORDS[alert['kind']]} since {started} Pacific time ({minutes} minutes so far)."
    else:
        subject = f"DockWatch: resolved, {name}"
        body = f"{name} is back to normal after {minutes} minutes (started {started} Pacific time)."
    return subject[:100], body  # SNS subjects are limited to 100 characters


def lambda_handler(event: dict, context=None) -> dict:
    topic_arn = os.environ.get("DOCKWATCH_ALERTS_TOPIC_ARN")
    stations = event.get("stations", {})
    sent = 0
    sns = None
    if topic_arn:
        import boto3

        sns = boto3.client("sns")
    for alert in event.get("alerts", []):
        subject, body = format_alert(alert, stations.get(alert["station_id"]))
        if sns is not None:
            sns.publish(TopicArn=topic_arn, Subject=subject, Message=body)
        else:
            print(json.dumps({"subject": subject, "body": body}))
        sent += 1
    return {"sent": sent}
