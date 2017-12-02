package de.metanome.algorithms.bellbrockhausen.accessors;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class PostgresTableInfoFactory implements TableInfoFactory {

    public TableInfo getTableInfo(final DatabaseConnectionGenerator connectionGenerator, final String tableName)
            throws AlgorithmExecutionException {
        ImmutableList<String> columnNames = getColumnNames(connectionGenerator, tableName);
        List<Attribute> attributes = new ArrayList<>();
        for (String columnName : columnNames) {
            attributes.add(getValueRange(connectionGenerator, tableName, columnName));
        }
        return new TableInfo(tableName, ImmutableList.copyOf(attributes));
    }

    private ImmutableList<String> getColumnNames(
            final DatabaseConnectionGenerator connectionGenerator,
            final String tableName)
            throws AlgorithmExecutionException {
        String query = format("SELECT column_name FROM information_schema.columns WHERE table_name='%s'", tableName);
        ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query);
        List<String> columnNames = new ArrayList<>();
        try {
            while (resultSet.next()) {
                columnNames.add(resultSet.getString("column_name"));
            }
        } catch (SQLException e) {
            throw new InputGenerationException(format("Error fetching column names of table %s", tableName), e);
        }
        return ImmutableList.copyOf(columnNames);
    }

    private Attribute getValueRange(
            final DatabaseConnectionGenerator connectionGenerator,
            final String tableName,
            final String columnName)
            throws AlgorithmExecutionException {
        String query = format("SELECT MIN(%s) as minVal, MAX(%s) as maxVal FROM %s", columnName, columnName, tableName);
        ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query);
        try {
            return new Attribute(columnName, resultSet.getInt("minVal"), resultSet.getInt("maxVal"));
        } catch (SQLException e) {
            throw new InputGenerationException(
                    format("Error calculating value range for column %s of table %s", columnName, tableName), e);
        }
    }
}
