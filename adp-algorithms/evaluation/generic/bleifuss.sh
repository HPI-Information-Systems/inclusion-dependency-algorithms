#!/bin/bash
export EXECUTION_ID=$DATASET"_tblblfs"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/bleifuss/build/libs/bleifuss-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.demarchi.DeMarchiAlgorithm \
$DB \
--table-key TABLE \
--tables load:$DATASET/$DATASET.txt \
--output file:$EXECUTION_ID

