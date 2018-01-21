# Mind2

## Run with metanome-cli

To get support for temp files, build metanome-cli using [this patch (#4)](https://github.com/sekruse/metanome-cli/pull/4).

The algorithm requires a TableInput. `--temp` specifies the directory to store temporary files in. The algorithm config expects a UIND algorithm to use for the UIND generation.

The algorithm-config `INDEX` field specifies a column that contains a unique numeric index for each row. This is needed to compare different cells after their columns have been sorted by values. Such an index column can be created using `ALTER TABLE <table> ADD COLUMN "MIND2_INDEX" SERIAL PRIMARY KEY;`. This is a workaround and may get substituted with a better solution.

```
java \
-cp metanome-cli.jar:mind2.jar:postgresql.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.mind2.Mind2Algorithm \
--db-type postgresql \
--db-connection pgpass.txt \
--table-key TABLE \
--tables table1 table2 table3 \
--output print \
--temp /tmp/mind2
--algorithm-config ind-input-algorithm:DE_MARCHI INDEX:MIND2_INDEX \
```
