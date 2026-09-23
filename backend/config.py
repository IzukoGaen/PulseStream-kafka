import os
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.environ["BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET = os.environ["KAFKA_API_SECRET"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
SCHEMA_REGISTRY_API_KEY = os.environ["SCHEMA_REGISTRY_API_KEY"]
SCHEMA_REGISTRY_API_SECRET = os.environ["SCHEMA_REGISTRY_API_SECRET"]
CONSUMER_GROUP = os.environ["CONSUMER_GROUP"]

TOPICS = {
    "request-count": "request_count_per_window",
    "status-breakdown": "status_breakdown_per_window",
    "active-users": "active_users_per_window",
    "top-pages": "top_pages_per_window",
}
