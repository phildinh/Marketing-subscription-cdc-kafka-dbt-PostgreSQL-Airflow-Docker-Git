#!/bin/bash
# =============================================================
# run_dbt_tests.sh
# Local test runner — mirrors what GitHub Actions runs
# Run this before pushing to catch failures early
# =============================================================

set -e  # exit immediately if any command fails

echo "Running dbt CI locally"
echo "======================"

cd dbt

echo ""
echo "Step 1 — dbt debug"
dbt debug

echo ""
echo "Step 2 — dbt run"
dbt run

echo ""
echo "Step 3 — dbt test"
dbt test

echo ""
echo "All checks passed"