#!/bin/bash
export EXECUTION_ID="tesma_binder"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/binder/build/libs/binder-0.1.0-SNAPSHOT-database.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.binder.BinderDatabaseAlgorithm \
$DB \
--table-key INPUT_DATABASE \
--tables load:tesma/tesma.txt \
--algorithm-config INPUT_ROW_LIMIT:-1,TEMP_FOLDER_PATH:binder_temp,CLEAN_TEMP:true \
--algorithm-config DETECT_NARY:false,MAX_NARY_LEVEL:-1,FILTER_KEY_FOREIGNKEYS:false \
--algorithm-config NUM_BUCKETS_PER_COLUMN:10,MEMORY_CHECK_FREQUENCY:1000,MAX_MEMORY_USAGE_PERCENTAGE:80 \
--output file:$EXECUTION_ID
