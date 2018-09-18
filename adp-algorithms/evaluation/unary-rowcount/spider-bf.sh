#!/usr/bin/env bash
export EXECUTION_ID="rowcount_spider-bf"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/spider-bruteforce/build/libs/spider-bruteforce-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.spiderbruteforce.SpiderBruteForceAlgorithm \
$DB \
--table-key TABLE \
--tables editor_sanitised_$@ \
--algorithm-config PROCESS_EMPTY_COLUMNS:true \
--algorithm-config INPUT_ROW_LIMIT:-1,MAX_MEMORY_USAGE_PERCENTAGE:80,MEMORY_CHECK_INTERVAL:1000 \
--output file:$EXECUTION_ID
