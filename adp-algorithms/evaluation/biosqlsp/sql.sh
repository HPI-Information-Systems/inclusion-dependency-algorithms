#!/bin/bash
export EXECUTION_ID="biosqlsp_sql"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/unary-sql/build/libs/unary-sql-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.unarysql.UnarySQLAlgorithm \
$DB \
--table-key TABLE \
--tables load:biosqlsp/biosqlsp.txt \
--output file:$EXECUTION_ID

