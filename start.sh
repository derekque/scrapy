#!/usr/bin/env bash
nohup python -u run.py > serve_`date +%Y%m%d%H%M%S`.out 2>&1 &