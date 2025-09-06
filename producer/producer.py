import os
import time
import json
import hashlib
import logging
from typing import Dict, Set, Tuple

import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load environment variables from .env file
load_dotenv()

# Configuration parameters loaded from environment or set with sensible defaults
AZURE_KEY = os.environ["AZURE_MAPS_API_KEY"]
BBOX = os.environ.get("BBOX", "-122.52,37.70,-122.35,37.83")  # Bounding box for San Francisco area
BOUNDING_ZOOM = int(os.environ.get("BOUNDING_ZOOM", "11"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic_incidents")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))

# Azure Maps Traffic Incident API endpoint and default query parameters
BASE = "https://atlas.microsoft.com/traffic/incident/detail/json"
PARAMS = {
    "api-version": "1.0",
    "style": "s3",
    "boundingbox": BBOX,              # Format: minLon,minLat,maxLon,maxLat
    "boundingZoom": str(BOUNDING_ZOOM),
    "projection": "EPSG4326",         # Standard GPS projection
    "trafficmodelid": "-1",           # Fetch the latest traffic model snapshot
    "subscription-key": AZURE_KEY
}

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def make_producer() -> Producer:
    """Initialize and return a Kafka producer using Confluent client."""
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def incident_key(model_id: str, inc_id: str) -> str:
    """
    Generate a unique hash key for each incident based on model ID and incident ID.
    Used for deduplication across polling cycles.
    """
    return hashlib.sha256(f"{model_id}:{inc_id}".encode()).hexdigest()

def flatten_incident(model_id: str, raw: Dict) -> Dict:
    """
    Normalize and flatten the raw Azure Maps incident data into a more readable schema.
    Preserves the original payload for traceability.
    """
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
        "_raw": raw,  # Retain full incident record for debugging or enrichment
    }
    # Use road name or endpoints as a fallback label for location context
    out["area_hint"] = out.get("road") or out.get("from") or out.get("to")
    return out

def poll_once(session: requests.Session) -> Tuple[str, list]:
    """
    Poll the Azure Maps API once and return the current traffic model ID
    along with a list of incident records.
    """
    r = session.get(BASE, params=PARAMS, timeout=15)
    r.raise_for_status()
    data = r.json()
    tm = data.get("tm", {})
    return tm.get("@id") or "", tm.get("poi", []) or []

def main():
    """Continuously poll Azure Maps and stream new incidents to Kafka."""
    producer = make_producer()
    session = requests.Session()
    seen: Set[str] = set()  # Track incident keys to avoid duplicates within this runtime

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

                # Generate unique key and skip if already seen
                key = incident_key(model_id, inc_id)
                if key in seen:
                    continue

                # Flatten the incident and send to Kafka
                event = flatten_incident(model_id, poi)
                producer.produce(KAFKA_TOPIC, value=json.dumps(event))
                seen.add(key)

            # Ensure all messages are sent before sleeping
            producer.flush()
        except Exception as e:
            logging.exception("Error: %s", e)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
