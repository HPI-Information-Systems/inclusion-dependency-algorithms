#!/bin/bash
export EXECUTION_ID="columncount_demarchi"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/demarchi/build/libs/demarchi-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.demarchi.DeMarchiAlgorithm \
$DB \
--table-key TABLE \
--tables load:unary-columncount/cath.txt \
--output file:$EXECUTION_ID
