# Mind2

## Run with metanome-cli

The algorithm requires a TableInput. `--temp` specifies the directory to store temporary files in. The algorithm config expects a UIND algorithm to use for the UIND generation.

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
--algorithm-config ind-input-algorithm:DE_MARCHI \
```

## Todos

- [ ] Fix hardcoded [IND input algorithm](https://github.com/HPI-Information-Systems/AdvancedDataProfilingSeminar/blob/e50b43137dbb382be60c4f7b64beb67dd8c67358/adp-algorithms/mind2/src/main/java/de/metanome/algorithms/mind2/Mind2Algorithm.java#L94).
- [x] Use SQL row number function instead of index column.
- [ ] Use `Set<Set<Int>>` instead of `Set<Set<IND>>` to handle max INDS for potentially faster set operations.
