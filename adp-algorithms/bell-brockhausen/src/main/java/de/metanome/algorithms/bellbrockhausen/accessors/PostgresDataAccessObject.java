package de.metanome.algorithms.bellbrockhausen.accessors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static de.metanome.util.UindUtils.getDependant;
import static de.metanome.util.UindUtils.getReferenced;
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
        return isInd(getDependant(candidate), getReferenced(candidate));
    }

    // TODO: Encapsulate schemaName and tableName to Class
    private String getTableName(String tableName) {
        if (tableName.contains(".")) {
            return tableName.split("\\.")[1];
        }
        return tableName;
    }

    private ImmutableList<ColumnIdentifier> getColumnNames(
            final DatabaseConnectionGenerator connectionGenerator,
            final String tableName)
            throws AlgorithmExecutionException {
        String query = format("SELECT column_name FROM information_schema.columns WHERE table_name='%s'", getTableName(tableName));
        List<ColumnIdentifier> columnNames = new ArrayList<>();
        try (ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query)) {
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
        String query = format("SELECT MIN(%s COLLATE \"C\") as minVal, MAX(%s COLLATE \"C\") as maxVal FROM %s",
                columnName, columnName, tableName);
        try (ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query)) {
            resultSet.next();
            final Range<Comparable> valueRange = extractRange(resultSet);
            return new Attribute(columnIdentifier, valueRange);
        } catch (SQLException e) {
            throw new InputGenerationException(
                    format("Error calculating value range for column %s", columnIdentifier), e);
        }
    }

    private Range<Comparable> extractRange(final ResultSet set) throws SQLException, AlgorithmExecutionException  {
        final Comparable min = (Comparable) set.getObject("minVal");
        final Comparable max = (Comparable) set.getObject("maxVal");
        return getRange(min, max);
    }

    private Range<Comparable> getRange(final Comparable min, final Comparable max) {
        if (min == null && max == null) {
            return Range.all();
        }

        if (min == null) {
            return Range.atMost(max);
        }

        if (max == null) {
            return Range.atLeast(min);
        }

        if (min.equals(max)) {
            return Range.singleton(min);
        }

        return Range.closed(min, max);
    }

    private boolean isInd(final ColumnIdentifier dependant, final ColumnIdentifier referenced)
            throws AlgorithmExecutionException {
        String query = format("SELECT shared, dependant FROM " +
                "(SELECT COUNT(DISTINCT(a.%s)) as shared FROM %s a, %s b WHERE a.%s = b.%s) AS q1, " +
                "(SELECT COUNT(DISTINCT(%s)) as dependant FROM %s) AS q2",
                dependant.getColumnIdentifier(), dependant.getTableIdentifier(), referenced.getTableIdentifier(),
                dependant.getColumnIdentifier(), referenced.getColumnIdentifier(),
                dependant.getColumnIdentifier(), dependant.getTableIdentifier());
        try (ResultSet resultSet = connectionGenerator.generateResultSetFromSql(query)) {
            resultSet.next();
            int sharedCount = resultSet.getInt("shared");
            int dependantCount = resultSet.getInt("dependant");
            return sharedCount == dependantCount;
        } catch (SQLException e) {
            throw new InputGenerationException(format("Error getting distinct value count for column %s and %s",
                    dependant.getColumnIdentifier(), referenced.getColumnIdentifier()));
        }
    }
}
