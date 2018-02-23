#!/usr/bin/env bash
export EXECUTION_ID="scop_mind"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/mind/build/libs/mind-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.mind.MindAlgorithm \
$DB \
--table-key TABLE \
--tables load:scop/scop.txt \
--algorithm-config MAX_DEPTH:-1,MAX_IND:true \
--output file:$EXECUTION_ID

