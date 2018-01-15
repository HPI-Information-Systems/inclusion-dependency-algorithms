package de.metanome.util;

import static java.util.stream.Collectors.toList;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import org.mockito.internal.stubbing.answers.ReturnsElementsOf;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TableInputGeneratorStub implements TableInputGenerator {

  private final String relationName;
  private final List<String> columnNames;
  private final List<Row> rows;

  @Builder
  public TableInputGeneratorStub(
          final String relationName,
          @Singular final List<String> columnNames,
          @Singular final List<Row> rows) {
    this.relationName = relationName;
    this.columnNames = columnNames;
    this.rows = rows;
  }

  @Override
  public ResultSet sortBy(String column, Boolean descending)
      throws InputGenerationException, AlgorithmConfigurationException {

    final int index = columnNames.indexOf(column);
    final List<String> values = rows.stream()
        .map(r -> r.getValues().get(index))
        .sorted(Comparator.nullsFirst(Comparator.naturalOrder()))
        .collect(toList());

    try {
      final ResultSet resultSet = mock(ResultSet.class);
      given(resultSet.next()).willAnswer(new NextAnswer(values.size()));
      given(resultSet.getString(column)).willAnswer(new ReturnsElementsOf(values));
      return resultSet;
    } catch (final SQLException e) {
      throw new InputGenerationException("Error", e);
    }
  }

  @AllArgsConstructor
  private static class NextAnswer implements Answer<Boolean> {

    private int counter;

    @Override
    public Boolean answer(final InvocationOnMock invocation) throws Throwable {
      return counter-- != 0;
    }
  }

  @Override
  public ResultSet filter(String filterExpression)
      throws InputGenerationException, AlgorithmConfigurationException {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ResultSet select() throws InputGenerationException, AlgorithmConfigurationException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public DatabaseConnectionGenerator getDatabaseConnectionGenerator() {
    return null;
  }

  @Override
  public RelationalInput generateNewCopy()
      throws InputGenerationException, AlgorithmConfigurationException {

    return new RelationalInputStub(relationName, columnNames, rows);
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}
