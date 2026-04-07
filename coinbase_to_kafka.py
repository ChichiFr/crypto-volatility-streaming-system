"""
Real-time cryptocurrency candle collector.
Connects to Coinbase Advanced Trade WebSocket and streams 1-minute candle data to Kafka.
"""

import json
import os
import sys
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-stream")
PRODUCT_IDS = [
    product.strip()
    for product in os.getenv("PRODUCT_IDS", "BTC-USD,ETH-USD").split(",")
    if product.strip()
]
COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com")

# ============================================================
# OUTPUT SETUP
# ============================================================

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ============================================================
# KAFKA PRODUCER SETUP
# ============================================================

print("[Kafka] Connecting to Kafka...")

producer = KafkaProducer(
    bootstrap_servers=[server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",") if server.strip()],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"[Kafka] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
print(f"[Config] Topic: {KAFKA_TOPIC}")
print(f"[Config] Tracking: {', '.join(PRODUCT_IDS)}\n")

# ============================================================
# WEBSOCKET CALLBACK FUNCTIONS
# ============================================================

# Keep in-progress 1-minute candles per product, keyed by minute start, and
# flush only after heartbeats confirm the minute has completed.
current_candles = {}


def emit_candle(candle):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": candle["symbol"],
        "interval_start": int(candle["start"]),
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
        "volume": float(candle["volume"]),
        "source": "coinbase-advanced-trade",
        "granularity": "1m",
    }

    producer.send(KAFKA_TOPIC, key=record["symbol"], value=record)
    print(
        f"[Data] {record['timestamp'][:19]} | "
        f"{record['symbol']:8} | "
        f"${record['close']:>10,.2f} | "
        f"Vol: {record['volume']:>12.6f}"
    )


def on_message(ws, message):
    """
    Called automatically when new data arrives from Coinbase.
    Builds and emits completed 1-minute candles to Kafka.
    """
    try:
        data = json.loads(message)
        channel = data.get("channel")

        if channel == "heartbeats":
            flush_completed_candles(data)
            return

        if channel != "market_trades":
            return

        for event in data.get("events", []):
            for trade in event.get("trades", []):
                process_trade(trade)

    except Exception as error:
        print(f"[Error] Error processing message: {error}")


def process_trade(trade):
    product_id = trade["product_id"]
    trade_time = parse_trade_time(trade["time"])
    minute_start = int(trade_time.timestamp()) // 60 * 60
    price = float(trade["price"])
    size = float(trade["size"])

    product_candles = current_candles.setdefault(product_id, {})
    candle = product_candles.get(minute_start)

    if candle is None:
        product_candles[minute_start] = {
            "symbol": product_id,
            "start": minute_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": size,
        }
        return

    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["volume"] += size


def flush_completed_candles(data):
    current_minute = None

    for event in data.get("events", []):
        current_time = event.get("current_time")
        if current_time:
            current_minute = int(parse_heartbeat_time(current_time).timestamp()) // 60 * 60
            break

    if current_minute is None:
        return

    empty_symbols = []

    for symbol, candles_by_minute in current_candles.items():
        completed_minutes = [
            minute_start for minute_start in candles_by_minute if minute_start < current_minute
        ]

        for minute_start in sorted(completed_minutes):
            emit_candle(candles_by_minute.pop(minute_start))

        if not candles_by_minute:
            empty_symbols.append(symbol)

    for symbol in empty_symbols:
        current_candles.pop(symbol, None)


def parse_trade_time(timestamp):
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


def parse_heartbeat_time(timestamp):
    normalized = timestamp.replace(" UTC", "").split(" m=+", 1)[0]
    main_part, timezone_part = normalized.rsplit(" ", 1)

    if "." in main_part:
        seconds_part, fractional_part = main_part.split(".", 1)
        main_part = f"{seconds_part}.{fractional_part[:6].ljust(6, '0')}"
    else:
        main_part = f"{main_part}.000000"

    parsed = datetime.strptime(f"{main_part} {timezone_part}", "%Y-%m-%d %H:%M:%S.%f %z")
    return parsed.astimezone(timezone.utc)


def on_error(ws, error):
    """
    Called when WebSocket encounters an error.
    """
    print(f"[Error] WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    """
    Called when connection closes.
    """
    print("\n[WebSocket] Connection closed")
    print(f"   Status: {close_status_code}")
    print(f"   Message: {close_msg}")

    producer.flush()
    print("[Kafka] All messages sent to Kafka")


def on_open(ws):
    """
    Called when connection first opens.
    Subscribes to Coinbase market trade and heartbeat channels.
    """
    print("[WebSocket] Connection opened")

    market_trades_subscription = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channel": "market_trades",
    }
    heartbeat_subscription = {
        "type": "subscribe",
        "channel": "heartbeats",
    }

    ws.send(json.dumps(market_trades_subscription))
    ws.send(json.dumps(heartbeat_subscription))

    print(f"[WebSocket] Subscribed to market trades: {', '.join(PRODUCT_IDS)}")
    print("[WebSocket] Heartbeats enabled to keep the connection alive")
    print("[WebSocket] Building completed 1-minute candles from trades...\n")
    print("=" * 80)


# ============================================================
# MAIN FUNCTION
# ============================================================


def start_streaming():
    """
    Create WebSocket connection and start streaming.
    """
    print("\n" + "=" * 80)
    print("CRYPTO REAL-TIME STREAMING SYSTEM")
    print("=" * 80)
    print(f"Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Products: {', '.join(PRODUCT_IDS)}")
    print(f"Coinbase WebSocket: {COINBASE_WS_URL}")
    print("=" * 80 + "\n")

    ws = websocket.WebSocketApp(
        COINBASE_WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )

    print("[WebSocket] Starting connection...\n")
    ws.run_forever()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        start_streaming()
    except KeyboardInterrupt:
        print("\n\n[System] Stopped by user (Ctrl+C)")
        print("[System] Goodbye!")
    except Exception as error:
        print(f"\n[Error] Fatal error: {error}")
        import traceback

        traceback.print_exc()
