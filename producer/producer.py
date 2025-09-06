import os, time, json, hashlib, logging, requests
from typing import Dict, Set, Tuple
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables from .env file for configuration
load_dotenv()

# Azure Maps API key and other configurable parameters with sane defaults
AZURE_KEY = os.environ["AZURE_MAPS_SUBSCRIPTION_KEY"]
BBOX = os.environ.get("BBOX", "-122.52,37.70,-122.35,37.83")  # Geographic bounding box (San Francisco area by default)
BOUNDING_ZOOM = int(os.environ.get("BOUNDING_ZOOM", "11"))     # Zoom level affects incident granularity
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")  # Kafka broker address
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "traffic_incidents")       # Kafka topic to send events to
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))                # Poll interval to avoid rate limiting

# Azure Maps Traffic Incident API endpoint and query parameters
BASE = "https://atlas.microsoft.com/traffic/incident/detail/json"
PARAMS = {
    "api-version": "1.0",
    "style": "s3",                      # Response style, compact
    "boundingbox": BBOX,                # minLon,minLat,maxLon,maxLat for focused traffic data
    "boundingZoom": str(BOUNDING_ZOOM),
    "projection": "EPSG4326",           # Standard geographic coordinate system
    "trafficmodelid": "-1",             # Use latest traffic snapshot available
    "subscription-key": AZURE_KEY       # Required API key for authorization
}

# Set up logging to output timestamps and log levels for easier debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def make_producer():
    """
    Create and return a KafkaProducer instance configured to serialize messages as JSON.
    'linger_ms' batches messages for 50ms to improve throughput.
    'acks=1' ensures leader broker acknowledges messages for reasonable durability.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="1",
    )

def incident_key(model_id: str, inc_id: str) -> str:
    """
    Generate a unique hash key for a traffic incident combining the traffic model and incident IDs.
    This helps deduplicate events already sent to Kafka.
    """
    return hashlib.sha256(f"{model_id}:{inc_id}".encode()).hexdigest()

def flatten_incident(model_id: str, raw: Dict) -> Dict:
    """
    Extract and normalize relevant fields from raw Azure Maps incident data.
    Adds an 'area_hint' for convenience in downstream processing or filtering.
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
        "clusterBounds": raw.get("cbl"),
        "clusterCenter": raw.get("ctr"),
        "clusterSize": raw.get("cs"),
        "lengthMeters": raw.get("l"),
        "_raw": raw,  # Keep full raw record for debugging or extended analysis
    }
    # Provide a simple geographic or road-based hint for quick identification
    out["area_hint"] = out.get("road") or out.get("from") or out.get("to")
    return out

def poll_once(session: requests.Session) -> Tuple[str, list]:
    """
    Perform a single API call to Azure Maps traffic incidents endpoint.
    Returns the current traffic model ID and a list of points-of-interest (incidents).
    Raises on HTTP errors to be handled by the caller.
    """
    r = session.get(BASE, params=PARAMS, timeout=15)
    r.raise_for_status()
    data = r.json()
    tm = data.get("tm", {})
    return tm.get("@id") or "", tm.get("poi", []) or []

def main():
    """
    Main event loop:
    - Initialize Kafka producer and HTTP session.
    - Track already seen incidents to avoid duplicates.
    - Poll Azure Maps API at fixed intervals, flatten incidents, send new ones to Kafka.
    - Log key events and handle errors gracefully.
    """
    producer = make_producer()
    session = requests.Session()
    seen: Set[str] = set()

    logging.info("Starting Azure Maps producer | BBOX=%s | ZOOM=%s | TOPIC=%s", BBOX, BOUNDING_ZOOM, KAFKA_TOPIC)

    while True:
        try:
            model_id, pois = poll_once(session)
            if not model_id:
                logging.warning("No traffic model id; skipping this poll.")
            else:
                logging.info("TrafficModelID=%s | incidents=%d", model_id, len(pois))

            for poi in pois:
                inc_id = poi.get("id")
                if not inc_id:
                    continue  # Skip malformed records without ID
                key = incident_key(model_id, inc_id)
                if key in seen:
                    continue  # Already processed this incident, skip

                event = flatten_incident(model_id, poi)
                producer.send(KAFKA_TOPIC, value=event)  # Send to Kafka topic
                seen.add(key)  # Mark as seen to avoid duplicates in future polls

            producer.flush(timeout=2)  # Ensure messages are sent promptly
        except requests.HTTPError as e:
            logging.error("HTTP error %s while polling Azure Maps API | body=%s", e, getattr(e.response, "text", "")[:300])
        except Exception as e:
            logging.exception("Unexpected error in main loop: %s", e)

        time.sleep(POLL_SECONDS)  # Wait before polling again to respect API limits

if __name__ == "__main__":
    main()
