"""
consumer.py
Consumes raw messages from 'raw-flights', applies data-quality rules,
and routes each record to one of two output topics:
  - valid-flights   : clean, well-formed records
  - invalid-flights : any record that fails validation
"""

import json
from kafka import KafkaConsumer, KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"
INPUT_TOPIC     = "raw-flights"
VALID_TOPIC     = "valid-flights"
INVALID_TOPIC   = "invalid-flights"
GROUP_ID        = "data-quality-consumer"

# Required keys every valid record must have
REQUIRED_KEYS   = {"ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_json_safely(raw: str):
    """
    Attempt to parse a raw string as JSON.
    Returns (dict, None) on success or (None, error_message) on failure.
    """
    try:
        return json.loads(raw), None
    except json.JSONDecodeError as exc:
        return None, f"JSONDecodeError: {exc}"


def validate_record(record: dict):
    """
    Apply all data-quality rules to a parsed record.
    Returns a list of violation strings (empty list = record is valid).

    Rules enforced:
      1. All required keys must be present.
      2. ORIGIN_COUNTRY_NAME must be a non-empty string (not null).
      3. DEST_COUNTRY_NAME must be a non-empty string (not null).
      4. count must be a non-negative integer (no strings, no nulls, no negatives).
      5. No unexpected extra keys are allowed.
    """
    violations = []

    # ── Rule 1 : required keys present ───────────────────────────────────────
    missing = REQUIRED_KEYS - record.keys()
    if missing:
        violations.append(f"Missing required keys: {missing}")
        # Cannot continue checking missing fields
        return violations

    # ── Rule 2 : ORIGIN_COUNTRY_NAME ─────────────────────────────────────────
    origin = record.get("ORIGIN_COUNTRY_NAME")
    if origin is None:
        violations.append("ORIGIN_COUNTRY_NAME is null")
    elif not isinstance(origin, str):
        violations.append(f"ORIGIN_COUNTRY_NAME must be a string, got {type(origin).__name__}")
    elif origin.strip() == "":
        violations.append("ORIGIN_COUNTRY_NAME is an empty string")

    # ── Rule 3 : DEST_COUNTRY_NAME ───────────────────────────────────────────
    dest = record.get("DEST_COUNTRY_NAME")
    if dest is None:
        violations.append("DEST_COUNTRY_NAME is null")
    elif not isinstance(dest, str):
        violations.append(f"DEST_COUNTRY_NAME must be a string, got {type(dest).__name__}")
    elif dest.strip() == "":
        violations.append("DEST_COUNTRY_NAME is an empty string")

    # ── Rule 4 : count ───────────────────────────────────────────────────────
    count = record.get("count")
    if count is None:
        violations.append("count is null")
    elif isinstance(count, bool):
        # bool is a subclass of int in Python — reject it explicitly
        violations.append("count must be an integer, got bool")
    elif isinstance(count, str):
        violations.append(f"count must be an integer, got string: '{count}'")
    elif not isinstance(count, int):
        violations.append(f"count must be an integer, got {type(count).__name__}")
    elif count < 0:
        violations.append(f"count must be >= 0, got {count}")

    # ── Rule 5 : no extra keys ───────────────────────────────────────────────
    extra_keys = record.keys() - REQUIRED_KEYS
    if extra_keys:
        violations.append(f"Unexpected extra keys: {extra_keys}")

    return violations


def enrich_invalid(raw: str, reason: str) -> str:
    """Wrap an invalid message with metadata before sending to invalid-flights."""
    payload = {
        "raw": raw,
        "reason": reason,
    }
    return json.dumps(payload)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode("utf-8"),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    stats = {"total": 0, "valid": 0, "invalid": 0}

    print(f"[Consumer] Listening on '{INPUT_TOPIC}' ...")

    for message in consumer:
        raw: str = message.value
        stats["total"] += 1

        # ── Step 1 : parse JSON safely ────────────────────────────────────────
        record, parse_error = parse_json_safely(raw)

        if parse_error:
            # Not valid JSON at all — route to invalid topic
            stats["invalid"] += 1
            producer.send(INVALID_TOPIC, value=enrich_invalid(raw, parse_error))
            print(f"[INVALID — parse error] {raw[:60]}  →  {parse_error}")
            continue

        # ── Step 2 : validate schema & data types ─────────────────────────────
        violations = validate_record(record)

        if violations:
            stats["invalid"] += 1
            reason = " | ".join(violations)
            producer.send(INVALID_TOPIC, value=enrich_invalid(raw, reason))
            print(f"[INVALID — {len(violations)} violation(s)] {raw[:60]}  →  {reason}")
        else:
            stats["valid"] += 1
            producer.send(VALID_TOPIC, value=json.dumps(record))
            print(f"[VALID]   {record['ORIGIN_COUNTRY_NAME']} → {record['DEST_COUNTRY_NAME']}  count={record['count']}")

        producer.flush()

    # Summary (reached only if consumer stops)
    print("\n── Summary ──────────────────────────────────")
    print(f"  Total   : {stats['total']}")
    print(f"  Valid   : {stats['valid']}")
    print(f"  Invalid : {stats['invalid']}")


if __name__ == "__main__":
    main()
