package de.metanome.util;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.ArrayList;
import java.util.List;

public class InclusionDependencyBuilder {

  private ColumnPermutation dependent;
  private ColumnPermutation referenced;


  public static class ColumnPermutationBuilder {

    private final InclusionDependencyBuilder outer;
    private final List<ColumnIdentifier> columns;

    ColumnPermutationBuilder(final InclusionDependencyBuilder outer) {
      this.outer = outer;
      columns = new ArrayList<>();
    }

    public ColumnPermutationBuilder column(final String relationName, final String columnName) {
      columns.add(new ColumnIdentifier(relationName, columnName));
      return this;
    }

    public ColumnPermutationBuilder columns(final String relationName,
        final List<String> columnNames) {

      for (final String columnName : columnNames) {
        columns.add(new ColumnIdentifier(relationName, columnName));
      }
      return this;
    }

    public ColumnPermutationBuilder referenced() {
      outer.dependent = columnPermutation();
      return new ColumnPermutationBuilder(outer);
    }

    public InclusionDependency build() {
      outer.referenced = columnPermutation();
      return outer.build();
    }

    private ColumnPermutation columnPermutation() {
      return new ColumnPermutation(columns.toArray(new ColumnIdentifier[columns.size()]));
    }
  }

  public InclusionDependency build() {
    return new InclusionDependency(dependent, referenced);
  }

  public static ColumnPermutationBuilder dependent() {
    return new ColumnPermutationBuilder(new InclusionDependencyBuilder());
  }
}
