package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;

public interface DataAccessObject {

    RelationalInput getSortedRelationalInput(
            TableInputGenerator inputGenerator,
            ColumnIdentifier sortColumn,
            ColumnIdentifier indexColumn,
            String indexColumnName,
            boolean descending) throws AlgorithmExecutionException;
}
