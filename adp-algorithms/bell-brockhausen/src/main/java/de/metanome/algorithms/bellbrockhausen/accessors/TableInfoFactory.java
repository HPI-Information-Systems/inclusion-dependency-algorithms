package de.metanome.algorithms.bellbrockhausen.accessors;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;

public interface TableInfoFactory {

    TableInfo getTableInfo(DatabaseConnectionGenerator connectionGenerator, String tableName)
        throws AlgorithmExecutionException;
}
