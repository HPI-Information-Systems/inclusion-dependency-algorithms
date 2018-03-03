#!/usr/bin/env bash

#!/bin/bash
export EXECUTION_ID="rowcount_demarchi"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/demarchi/build/libs/demarchi-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.demarchi.DeMarchiAlgorithm \
$DB \
--table-key TABLE \
--tables load:unary-rowcount/rowcount.txt \
--algorithm-config INPUT_ROW_LIMIT:$@ \
--output file:$EXECUTION_ID

