#!/bin/bash

python /tmp/main.py \
  --query "$1" \
  --pg_config "$2" \
  --detuple "$3" \
  --payload "$4" \
  --output "$5"
