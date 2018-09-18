#!/bin/bash

function run_algorithm {
  reload $1
  seq 1 3 | xargs -Irun timeout --kill-after 5m 4h ./generic/spider-bruteforce.sh run
}

function reload {
  echo $1
  export DATASET=$1
  source evaluation_env.sh
}

run_algorithm "scop"
run_algorithm "census"
run_algorithm "wikipedia"
run_algorithm "biosqlsp"
run_algorithm "wikirank"
run_algorithm "lod"
run_algorithm "ensembl"
run_algorithm "cath"
run_algorithm "tesma"
run_algorithm "tpch1s"
run_algorithm "tpch10s"
run_algorithm "musicbrainz"
