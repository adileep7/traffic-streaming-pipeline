import os
import time
import json
import hashlib
import logging
from typing import Dict, Set, Tuple

import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load environment variables
load_dotenv()

AZURE_KEY = os.environ["AZURE_MAPS_API_KEY"]
BBOX = os.environ.get("BBOX", "-122.52,37.70,-122.35,37.83")
BOUNDING_ZOOM = int(os.environ.get("BOUNDING_ZOOM", "11"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic_incidents")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))

# Azure Maps Traffic Incident API endpoint
BASE = "https://atlas.microsoft.com/traffic/incident/detail/json"
PARAMS = {
    "api-version": "1.0",
    "style": "s3",
    "boundingbox": BBOX,          # minLon,minLat,maxLon,maxLat
    "boundingZoom": str(BOUNDING_ZOOM),
    "projection": "EPSG4326",
    "trafficmodelid": "-1",       # always fetch the latest model
    "subscription-key": AZURE_KEY
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def incident_key(model_id: str, inc_id: str) -> str:
    """Unique key for deduplication across models."""
    return hashlib.sha256(f"{model_id}:{inc_id}".encode()).hexdigest()

def flatten_incident(model_id: str, raw: Dict) -> Dict:
    """Extract a simplified schema from Azure Maps incident data."""
    out = {
        "trafficModelId": model_id,
        "id": raw.get("id"),
        "typeCode": raw.get("ty"),
        "iconCode": raw.get("ic"),
        "description": raw.get("d"),
        "comment": raw.get("c"),
        "from": raw.get("f"),
        "to": raw.get("t"),
        "road": raw.get("r"),
        "delaySeconds": raw.get("dl"),
        "position": raw.get("p"),
        "_raw": raw,  # keep full original object
    }
    out["area_hint"] = out.get("road") or out.get("from") or out.get("to")
    return out

def poll_once(session: requests.Session) -> Tuple[str, list]:
    """Fetch one batch of incidents from Azure Maps."""
    r = session.get(BASE, params=PARAMS, timeout=15)
    r.raise_for_status()
    data = r.json()
    tm = data.get("tm", {})
    return tm.get("@id") or "", tm.get("poi", []) or []

def main():
    producer = make_producer()
    session = requests.Session()
    seen: Set[str] = set()

    logging.info("Starting Azure Maps producer | BBOX=%s | ZOOM=%s | TOPIC=%s",
                 BBOX, BOUNDING_ZOOM, KAFKA_TOPIC)

    while True:
        try:
            model_id, pois = poll_once(session)
            logging.info("TrafficModelID=%s | incidents=%d", model_id, len(pois))

            for poi in pois:
                inc_id = poi.get("id")
                if not inc_id:
                    continue
                key = incident_key(model_id, inc_id)
                if key in seen:
                    continue
                event = flatten_incident(model_id, poi)
                producer.produce(KAFKA_TOPIC, value=json.dumps(event))
                seen.add(key)

            # flush to ensure delivery
            producer.flush()
        except Exception as e:
            logging.exception("Error: %s", e)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
