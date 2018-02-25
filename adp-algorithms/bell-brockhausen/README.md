# Bell and Brockhausen

## Run with metanome-cli

```
java \
-cp metanome-cli.jar:bell-brockhausen.jar:postgresql.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.bellbrockhausen.BellBrockhausenAlgorithm \
--db-type postgresql \
--db-connection pgpass.txt \
--table-key TABLE \
--tables <table1> <table2> <table3> \
--output print \
```
