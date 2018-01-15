# Bell and Brockhausen

## Run with metanome-cli

To get support for database connections, build metanome-cli using [this patch (#3)](https://github.com/sekruse/metanome-cli/pull/3).

The tables to run the algorithm on are passed through the `TABLE` field of the algorithm config. Note that the `--tables` arg from metanome-cli can be ignored since it is not supported for database connections.

```
java \
-cp metanome-cli.jar:bell-brockhausen.jar:postgresql.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.bellbrockhausen.BellBrockhausenAlgorithm \
--db-type postgresql \
--db-connection pgpass.txt \
--table-key DATABASE \
--tables placeholder \
--algorithm-config TABLE:table1 TABLE:table2 TABLE:table3 \
--output print \
```
