#!/bin/bash
# usage: ./record.sh <variant> <run> <first_s> <subsequent_s|-> -> appends one browser measurement to results/browser_runs.jsonl
cd "$(dirname "$0")"
echo "{\"variant\":\"$1\",\"run\":$2,\"first_load_s\":$3,\"subsequent_load_s\":$( [ "$4" = "-" ] && echo null || echo $4 ),\"ts\":\"$(date +%H:%M:%S)\"}" >> results/browser_runs.jsonl
