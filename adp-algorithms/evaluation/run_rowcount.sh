#!/bin/bash

export DATASET=unary_evaluation_rowcount
source evaluation_env.sh

function run {
	echo $1
	seq 21 30 | xargs -Irun timeout -s kill 4h ./unary-rowcount/$1.sh run
}

run demarchi
run sindd
run spider
run spider-bf
run binder
#run bb
