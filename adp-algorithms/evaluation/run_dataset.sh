#!/bin/bash

export DATASET=deepsat6
source evaluation_env.sh

function run_dataset {
  seq 1 3 | xargs -Irun timeout -s kill 4h ./generic/$1.sh run
}

run_dataset bb
run_dataset binder
run_dataset demarchi
run_dataset spider
run_dataset spider-bf
run_dataset sindd
