package de.metanome.util;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.sql.ResultSet;
import java.util.function.Consumer;

public class AttributeHelper {

  /**
   * Get all distinct, non-null values for a single attribute.
   *
   * @param input the relational input
   * @param table the source table's name
   * @param column the attribute's name
   * @param inputRowLimit the max row count; {@code <= 0} to disable
   * @param consumer the consumer that processes a single attribute value at a time
   * @return if the attribute had at least one value
   */
  public boolean getValues(final RelationalInputGenerator input,
      final String table,
      final String column,
      final int inputRowLimit,
      final Consumer<String> consumer) throws AlgorithmExecutionException {

    if (input instanceof TableInputGenerator) {
      return getValuesFromDatabase((TableInputGenerator) input, table, column, inputRowLimit,
          consumer);
    }

    return getValuesFromRelationalInput(input, column, inputRowLimit, consumer);
  }

  private boolean getValuesFromRelationalInput(final RelationalInputGenerator inputGenerator,
      final String column,
      final int inputRowLimit,
      final Consumer<String> consumer) throws AlgorithmExecutionException {

    int rowCount = 0;
    boolean hasValue = false;

    try (RelationalInput input = inputGenerator.generateNewCopy()) {
      final int offset = input.columnNames().indexOf(column);
      while (input.hasNext()) {

        if (inputRowLimit > 0 && rowCount >= inputRowLimit) {
          break;
        }
        ++rowCount;

        final String value = input.next().get(offset);
        if (value != null) {
          hasValue = true;
          consumer.accept(value);
        }
      }

      return hasValue;
    } catch (final Exception e) {
      throw new InputGenerationException("reading attribute values", e);
    }
  }

  private boolean getValuesFromDatabase(final TableInputGenerator input,
      final String table,
      final String column,
      final int inputRowLimit,
      final Consumer<String> consumer) throws AlgorithmExecutionException {

    final String sql = buildValueQuery(table, column, inputRowLimit);
    try (DatabaseConnectionGenerator connection = input.getDatabaseConnectionGenerator();
        final ResultSet set = connection.generateResultSetFromSql(sql)) {
      boolean hasValue = false;
      while (set.next()) {
        hasValue = true;
        final String value = set.getString(1);
        consumer.accept(value);
      }
      return hasValue;
    } catch (final Exception e) {
      throw new AlgorithmExecutionException("getting attribute values failed", e);
    }
  }

  private String buildValueQuery(final String table, final String column,
      final int inputRowLimit) {

    /*
     * TODO: consider porting this to jOOQ
     * However, two obstacles are already apparent:
     *  - expressing a conditional limit clause is painful
     *  - jOOQ appeared to be very slow when retrieving larger amounts of data from the database
     */
    final StringBuffer buffer = new StringBuffer(50);
    buffer.append(String.format("WITH r AS (SELECT %s FROM %s ", column, table));
    if (inputRowLimit > 0) {
      buffer.append(String.format("LIMIT %d ", inputRowLimit));
    }
    buffer.append(") ");

    buffer.append(String.format("SELECT DISTINCT %s COLLATE \"C\" FROM r WHERE %s IS NOT NULL",
        column, column));
    return buffer.toString();
  }
}
