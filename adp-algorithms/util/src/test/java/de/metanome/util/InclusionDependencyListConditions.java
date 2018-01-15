package de.metanome.util;

import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.List;
import java.util.function.Predicate;
import org.assertj.core.api.Condition;

public class InclusionDependencyListConditions {

  public static Condition<List<? extends InclusionDependency>> unaryCountOf(final int count) {
    return new Condition<>(countWithLength(count, 1),
        "Count of unary inclusion dependencies should be %d", count);
  }

  public static Condition<List<? extends InclusionDependency>> binaryCountOf(final int count) {
    return new Condition<>(countWithLength(count, 2),
        "Count of binary inclusion dependencies should be %d", count);
  }

  private static Predicate<List<? extends InclusionDependency>> countWithLength(final int count,
      final int length) {

    return inds ->
        inds.stream().filter(ind -> ind.getDependant().getColumnIdentifiers().size() == length)
            .count() == count;
  }

}
