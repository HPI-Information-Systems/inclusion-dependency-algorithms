package de.metanome.util;

import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import java.util.Iterator;
import java.util.List;
import lombok.Builder;
import lombok.Singular;

public class RelationalInputStub implements RelationalInput {

  private final List<String> columnNames;
  private final Iterator<Row> rows;

  @Builder
  RelationalInputStub(@Singular final List<String> columnNames,
      @Singular final List<Row> rows) {
    this.columnNames = columnNames;
    this.rows = rows.iterator();
  }

  @Override
  public boolean hasNext() throws InputIterationException {
    return rows.hasNext();
  }

  @Override
  public List<String> next() throws InputIterationException {
    return rows.next().getValues();
  }

  @Override
  public int numberOfColumns() {
    return columnNames.size();
  }

  @Override
  public String relationName() {
    return "TEST";
  }

  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

}
