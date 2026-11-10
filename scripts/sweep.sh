#!/bin/sh
# The evaluation sweep behind results/SUMMARY.md. Run from the repository root:
#   sh scripts/sweep.sh
# Each run starts its own cluster, so the runs share no state. Results land in
# results/*.json.
set -eu
cd "$(dirname "$0")/.."

EVENTS=10000

# Every HTTP request opens a new TCP connection, and the closed ones sit in TIME_WAIT
# (30 seconds on macOS). Waiting for them to clear gives each run the same start.
settle() {
    while [ "$(netstat -an | grep -c TIME_WAIT)" -gt 2000 ]; do
        sleep 2
    done
}

# Partitions x acks, twice each. The two passes run one after the other, so a slow
# minute on the machine doesn't land on both copies of one setting.
for pass in 1 2; do
    for partitions in 1 3 6; do
        for acks in 1 2 3; do
            settle
            python3 -m eventlog bench --events "$EVENTS" --partitions "$partitions" \
                --acks "$acks" --threads 6
        done
    done
done

# Each fault once, at 3 partitions and acks=2.
for fault in kill-leader lost-replies slow-follower; do
    settle
    python3 -m eventlog bench --events "$EVENTS" --partitions 3 --acks 2 --threads 6 \
        --fault "$fault"
done

# One large run.
settle
python3 -m eventlog bench --events 200000 --partitions 6 --acks 2 --threads 6
