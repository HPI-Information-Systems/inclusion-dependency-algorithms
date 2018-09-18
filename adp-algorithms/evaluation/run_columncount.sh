#!/bin/bash

export DATASET=unary_evaluation_columncount
source evaluation_env.sh

function run {
	echo $1
	seq 1 20 | xargs -Irun timeout -s kill 4h ./unary-columncount/$1.sh run
}

run demarchi
run sindd
run spider
run spider-bf
run binder
run bb
