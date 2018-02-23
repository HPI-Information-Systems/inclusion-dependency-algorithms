#!/usr/bin/env bash
export EXECUTION_ID="scop_spider"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/spider/build/libs/spider-0.1.0-SNAPSHOT-database.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.spider.SpiderDatabaseAlgorithm \
$DB \
--table-key TABLE \
--tables load:scop/scop.txt \
--algorithm-config PROCESS_EMPTY_COLUMNS:true,INPUT_ROW_LIMIT:-1,MAX_MEMORY_USAGE_PERCENTAGE:50 \
--algorithm-config MEMORY_CHECK_INTERVAL:1000 \
--output file:$EXECUTION_ID
