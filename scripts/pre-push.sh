#!/bin/sh

BOLT_DRIVER_LOG=error NEO4J_BOLT=bolt://localhost:7687 go test -v || {
    echo "tests failed"
    exit 1;
}

echo "PrePush Success"
exit 0
