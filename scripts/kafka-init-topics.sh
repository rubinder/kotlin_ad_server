#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

for topic in bid-requests auction-results impression-events; do
    docker exec -it kotlin-ad-kafka kafka-topics \
        --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 6 \
        --replication-factor 1
done
echo "Topics created on $BOOTSTRAP"
