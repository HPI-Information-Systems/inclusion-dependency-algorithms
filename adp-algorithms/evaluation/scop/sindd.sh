#!/usr/bin/env bash
export EXECUTION_ID="scop_sindd"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/sindd/build/libs/sindd-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.sindd.SinddAlgorithm \
$DB \
--table-key TABLE \
--tables load:scop/scop.txt \
--algorithm-config PROCESS_EMPTY_COLUMNS:true,OPEN_FILE_NR:20,PARTITION_NR:1 \
--output file:$EXECUTION_ID
