#!/usr/bin/env bash
export EXECUTION_ID="tesma_bellbrockhausen"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/bell-brockhausen/build/libs/bell-brockhausen-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.bellbrockhausen.BellBrockhausenAlgorithm \
$DB \
--table-key TABLE \
--tables load:tesma/tesma.txt \
--output file:$EXECUTION_ID
