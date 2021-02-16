#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/golang-neo4j-bolt-driver
  make build
popd