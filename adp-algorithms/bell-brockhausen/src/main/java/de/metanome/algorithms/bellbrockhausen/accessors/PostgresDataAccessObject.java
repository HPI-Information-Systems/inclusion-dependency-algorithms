package de.metanome.algorithms.bellbrockhausen.accessors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;
import de.metanome.algorithms.bellbrockhausen.models.DataType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;

public class PostgresDataAccessObject implements DataAccessObject {

    private final DatabaseConnectionGenerator connectionGenerator;

    public PostgresDataAccessObject(DatabaseConnectionGenerator connectionGenerator) {
        this.connectionGenerator = connectionGenerator;
    }

    public TableInfo getTableInfo(final String tableName)
            throws AlgorithmExecutionException {
        ImmutableList<ColumnIdentifier> columnNames = getColumnNames(connectionGenerator, tableName);
        List<Attribute> attributes = new ArrayList<>();
        for (ColumnIdentifier columnName : columnNames) {
            attributes.add(getValueRange(connectionGenerator, columnName));
        }
        return new TableInfo(tableName, ImmutableList.copyOf(attributes));
    }

    @Override
    public boolean isValidUIND(InclusionDependency candidate) throws AlgorithmExecutionException {
        if (candidate.getDependant().getColumnIdentifiers().size() != 1 ||
                candidate.getDependant().getColumnIdentifiers().size() != 1) {
            throw new AlgorithmExecutionException(format("Algorithm can only handle UINDs. Got nIND: %s", candidate));
        }
        // TODO: Optimize this by combining checks for A \subseteq B and B \subseteq A
        int dependantCount = getDistinctValues(getOnlyElement(candidate.getDependant().getColumnIdentifiers()));
        int sharedCount = getDistinctValues(
                getOnlyElement(candidate.getDependant().getColumnIdentifiers()),
                getOnlyElement(candidate.getReferenced().getColumnIdentifiers()));
        return dependantCount == sharedCount;
    }

    // TODO: Encapsulate schemaName and tableName to Class
    private String getTableName(String tableName) {
        return tableName.split("\\.")[1];
    }

    private ImmutableList<ColumnIdentifier> getColumnNames(
            final DatabaseConnectionGenerator connectionGenerator,
            final String tableName)
            throws AlgorithmExecutionException {
        String query = format("SELECT column_name FROM information_schema.columns WHERE table_name='%s'", getTableName(tableName));
        ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query);
        List<ColumnIdentifier> columnNames = new ArrayList<>();
        try {
            while (resultSet.next()) {
                columnNames.add(new ColumnIdentifier(tableName, resultSet.getString("column_name")));
            }
        } catch (SQLException e) {
            throw new InputGenerationException(format("Error fetching column names of table %s", tableName), e);
        }
        return ImmutableList.copyOf(columnNames);
    }

    private Attribute getValueRange(final DatabaseConnectionGenerator connectionGenerator, final ColumnIdentifier columnIdentifier)
            throws AlgorithmExecutionException {
        String columnName = columnIdentifier.getColumnIdentifier();
        String tableName = columnIdentifier.getTableIdentifier();
        String query = format("SELECT MIN(%s) as minVal, MAX(%s) as maxVal, PG_TYPEOF(MAX(%s)) as type FROM %s",
                columnName, columnName, columnName, tableName);
        ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query);
        try {
            resultSet.next();
            DataType type = DataType.fromString(resultSet.getString("type"));
            Range<Comparable> valueRange;
            switch (type) {
                case TEXT:
                    valueRange = Range.closed(resultSet.getString("minVal"), resultSet.getString("maxVal"));
                    break;
                case INTEGER:
                    valueRange = Range.closed(resultSet.getInt("minVal"), resultSet.getInt("maxVal"));
                    break;
                default:
                    throw new AlgorithmExecutionException(
                            format("Invalid data type %s for column %s", type, columnIdentifier));
            }
            return new Attribute(columnIdentifier, valueRange, type);
        } catch (SQLException e) {
            throw new InputGenerationException(
                    format("Error calculating value range for column %s", columnIdentifier), e);
        }
    }

    private int getDistinctValues(final ColumnIdentifier column) throws AlgorithmExecutionException {
        String query = format("SELECT COUNT(DISTINCT(%s)) as values FROM %s",
                column.getColumnIdentifier(), column.getTableIdentifier());
        ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query);
        try {
            resultSet.next();
            return resultSet.getInt("values");
        } catch (SQLException e) {
            throw new InputGenerationException(format("Error getting distinct value count for column %s of table %s",
                        column.getColumnIdentifier(), column.getTableIdentifier()));
        }
    }

    private int getDistinctValues(final ColumnIdentifier columnA, final ColumnIdentifier columnB)
            throws AlgorithmExecutionException {
        String query = format("SELECT COUNT(DISTINCT(a.%s)) as values FROM %s a, %s b WHERE a.%s = b.%s",
                columnA.getColumnIdentifier(), columnA.getTableIdentifier(), columnB.getTableIdentifier(),
                columnA.getColumnIdentifier(), columnB.getColumnIdentifier());
        ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query);
        try {
            resultSet.next();
            return resultSet.getInt("values");
        } catch (SQLException e) {
            throw new InputGenerationException(format("Error getting distinct value count for column %s and %s",
                    columnA.getColumnIdentifier(), columnB.getColumnIdentifier()));
        }
    }
}
