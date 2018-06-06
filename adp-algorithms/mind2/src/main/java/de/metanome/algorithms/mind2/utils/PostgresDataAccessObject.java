package de.metanome.algorithms.mind2.utils;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.util.ResultSetIterator;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;

public class PostgresDataAccessObject implements DataAccessObject {

    private static final String SORT_STATEMENT = "SELECT *, row_number() OVER (ORDER BY %s COLLATE \"C\") AS %s FROM %s ORDER BY %s COLLATE \"C\" %s";

    @Override
    public RelationalInput getSortedRelationalInput(
            TableInputGenerator inputGenerator,
            ColumnIdentifier sortColumn,
            ColumnIdentifier indexColumn,
            String indexColumnName,
            boolean descending) throws AlgorithmExecutionException {
        try {
            ResultSet resultSet = sortBy(inputGenerator, sortColumn, indexColumn, indexColumnName, descending);
            return new ResultSetIterator(resultSet);
        } catch (SQLException | InputGenerationException e) {
            throw new AlgorithmExecutionException("Could not construct database input", e);
        }
    }

    private ResultSet sortBy(
            TableInputGenerator inputGenerator,
            ColumnIdentifier sortColumn,
            ColumnIdentifier indexColumn,
            String indexColumnName,
            boolean descending) throws InputGenerationException, AlgorithmConfigurationException {
        String descendingKey = descending ? "DESC" : "ASC";
        String query = format(SORT_STATEMENT, indexColumn.getColumnIdentifier(), indexColumnName,
                sortColumn.getTableIdentifier(), sortColumn.getColumnIdentifier(), descendingKey);
       return inputGenerator.getDatabaseConnectionGenerator().generateResultSetFromSql(query);
    }
}
