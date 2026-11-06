from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = APP_ROOT / "data"
METRICS_FILE = DATA_DIR / "live_metrics.json"
TOPIC = "gaming-events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

DATA_DIR.mkdir(exist_ok=True)
