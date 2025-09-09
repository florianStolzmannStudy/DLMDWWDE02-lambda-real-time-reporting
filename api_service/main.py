import logging
from datetime import datetime
from fastapi import FastAPI, Response, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)

app = FastAPI(title="API Service - Taxi Record")

class TaxiRecord(BaseModel):
    tpep_pickup_datetime: str

def make_record() -> TaxiRecord:
    return TaxiRecord(tpep_pickup_datetime=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

STREAM_REQUESTS = Counter("api_requests_total", "Anzahl der abgerufenen API-Requests")

@app.get("/health")
async def health():
    try:
        return {"ok": True}
    except Exception:
        log.exception("health failed")
        raise HTTPException(status_code=500, detail="internal error")

@app.get("/stream", response_model=TaxiRecord)
async def stream():
    try:
        STREAM_REQUESTS.inc()
        return make_record()
    except Exception:
        log.exception("stream failed")
        raise HTTPException(status_code=500, detail="internal error")

@app.get("/metrics")
async def metrics():
    try:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except Exception:
        log.exception("metrics failed")
        raise HTTPException(status_code=500, detail="internal error")
