#!/usr/bin/env bash

#!/bin/bash
export EXECUTION_ID="rowcount_demarchi"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/demarchi/build/libs/demarchi-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.demarchi.DeMarchiAlgorithm \
$DB \
--table-key TABLE \
--tables editor_sanitised_$@ \
--algorithm-config INPUT_ROW_LIMIT:-1 \
--output file:$EXECUTION_ID

