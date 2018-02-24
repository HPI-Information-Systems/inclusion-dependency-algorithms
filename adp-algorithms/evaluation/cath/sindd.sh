#!/usr/bin/env bash
export EXECUTION_ID="cath_sindd"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/sindd/build/libs/sindd-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.sindd.SinddAlgorithm \
$DB \
--table-key TABLE \
--tables load:cath/cath.txt \
--algorithm-config PROCESS_EMPTY_COLUMNS:true,OPEN_FILE_NR:100,PARTITION_NR:10 \
--algorithm-config INPUT_ROW_LIMIT:-1,MAX_MEMORY_USAGE_PERCENTAGE:70,MEMORY_CHECK_INTERVAL:1000 \
--output file:$EXECUTION_ID

rm -rv tmp/
