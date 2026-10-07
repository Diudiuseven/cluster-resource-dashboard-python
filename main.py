import uvicorn

from app.api import create_app
from app.service import DashboardService
from app.source import ReplayableSource

source = ReplayableSource(seed=42, num_resources=50, num_events=200)
service = DashboardService(source)
app = create_app(service, pump_interval=0.05)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
