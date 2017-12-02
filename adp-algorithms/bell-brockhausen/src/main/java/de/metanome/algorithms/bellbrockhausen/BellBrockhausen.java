package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.accessors.PostgresTableInfoFactory;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfoFactory;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;

public class BellBrockhausen {

    private final BellBrockhausenConfiguration configuration;
    private final TableInfoFactory tableInfoFactory;

    public BellBrockhausen(final BellBrockhausenConfiguration configuration) {
        this.configuration = configuration;
        this.tableInfoFactory = new PostgresTableInfoFactory(); // TODO: Inject database specific factory
    }

    public void execute() throws AlgorithmExecutionException {
        TableInfo tableInfo = tableInfoFactory.getTableInfo(
                configuration.getConnectionGenerator(), configuration.getTableName());
    }

    private void generateCandidates() {
    }
}
