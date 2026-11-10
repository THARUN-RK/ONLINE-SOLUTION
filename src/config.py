from pathlib import Path
import os

APP_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = APP_ROOT / "data"
METRICS_FILE = DATA_DIR / "live_metrics.json"
TOPIC = "gaming-events"
TOPIC_PLAYER_EVENTS = "player-events"
TOPIC_GAME_SCORES = "game-scores"
TOPIC_TRANSACTIONS = "transactions"
TOPIC_GAME_SESSIONS = "game-sessions"
TOPIC_ACHIEVEMENTS = "achievements"
TOPIC_PIPELINE_METRICS = "pipeline-metrics"
TOPIC_DEAD_LETTER = "gaming-events.dlq"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS.split(",")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://gaming:gaming@localhost:5432/gaming",
)
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "gaming-events-processor")
ACTIVE_WINDOW_MINUTES = int(os.getenv("ACTIVE_WINDOW_MINUTES", "5"))
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
EVENT_RATE = float(os.getenv("EVENT_RATE", "20"))
SIMULATED_PLAYERS = int(os.getenv("SIMULATED_PLAYERS", "250"))

DATA_DIR.mkdir(exist_ok=True)
