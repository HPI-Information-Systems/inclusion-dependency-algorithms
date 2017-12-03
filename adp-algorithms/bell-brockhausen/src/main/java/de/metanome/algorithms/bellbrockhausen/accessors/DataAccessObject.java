package de.metanome.algorithms.bellbrockhausen.accessors;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;

public interface DataAccessObject {

    TableInfo getTableInfo(String tableName)
        throws AlgorithmExecutionException;

    boolean isValidUIND(InclusionDependency candidate) throws AlgorithmExecutionException;
}
