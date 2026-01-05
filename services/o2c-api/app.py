import json
import os
import time
from functools import wraps

from flask import Flask, request, jsonify
import pika
import jwt

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = os.getenv("QUEUE_NAME", "o2c.events")
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
JWT_ALG = "HS256"

# --- Prometheus metrics ---
HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"]
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["endpoint"]
)

# Demo users (MVP) - in real life use DB/IdP
USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "user": {"password": "user123", "role": "user"},
}

def issue_token(username: str, role: str) -> str:
    now = int(time.time())
    payload = {"sub": username, "role": role, "iat": now, "exp": now + 3600}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

def decode_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])

def require_auth(allowed_roles=None):
    allowed_roles = allowed_roles or []

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                return jsonify({"error": "Missing Bearer token"}), 401
            token = auth.split(" ", 1)[1].strip()
            try:
                claims = decode_token(token)
            except Exception:
                return jsonify({"error": "Invalid/expired token"}), 401

            if allowed_roles and claims.get("role") not in allowed_roles:
                return jsonify({"error": "Forbidden"}), 403

            request.claims = claims
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def publish_event(event: dict) -> None:
    for _ in range(20):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(event).encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            connection.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Unable to connect to RabbitMQ")

@app.before_request
def _start_timer():
    request._t0 = time.time()

@app.after_request
def _record_metrics(resp):
    try:
        endpoint = request.path
        method = request.method
        status = str(resp.status_code)
        HTTP_REQUESTS.labels(method=method, endpoint=endpoint, status=status).inc()
        if hasattr(request, "_t0"):
            HTTP_LATENCY.labels(endpoint=endpoint).observe(time.time() - request._t0)
    except Exception:
        pass
    return resp

@app.get("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/login")
def login():
    payload = request.get_json(force=True) or {}
    username = payload.get("username")
    password = payload.get("password")

    user = USERS.get(username)
    if not user or user["password"] != password:
        return jsonify({"error": "Invalid credentials"}), 401

    token = issue_token(username, user["role"])
    return jsonify({"access_token": token, "role": user["role"]})

@app.post("/orders")
@require_auth(allowed_roles=["admin", "user"])
def create_order():
    payload = request.get_json(force=True) or {}

    order = {
        "order_id": payload.get("order_id"),
        "customer_id": payload.get("customer_id"),
        "currency": payload.get("currency", "USD"),
        "amount": float(payload.get("amount", 0)),
        "created_at": int(time.time()),
        "created_by": request.claims.get("sub"),
    }

    event = {
        "event_type": "SalesOrderCreated",
        "event_version": "1.0",
        "data": order,
    }

    publish_event(event)

    return jsonify({"message": "Order received and event published", "event": event}), 201

@app.get("/admin/queue")
@require_auth(allowed_roles=["admin"])
def admin_queue_info():
    # Simple admin-only endpoint to demonstrate RBAC (MVP)
    return jsonify({"queue": QUEUE_NAME, "rabbitmq_host": RABBITMQ_HOST})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
