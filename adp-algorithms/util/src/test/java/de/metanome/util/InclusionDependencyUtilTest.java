package de.metanome.util;


import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InclusionDependencyUtilTest {

  private static final String TABLE = "table";

  private InclusionDependencyUtil util;

  @BeforeEach
  void setUp() {
    util = new InclusionDependencyUtil();
  }

  @Test
  void maxInd_unrelatedColumns() {
    final List<InclusionDependency> toCheck = asList(
        ind("wx", "yz"),
        a_b(TABLE));

    final List<InclusionDependency> expected = new ArrayList<>(toCheck);
    assertThat(util.getMax(toCheck)).hasSameElementsAs(expected);
  }

  @Test
  void maxInd_sharedColumn_contained() {
    final List<InclusionDependency> toCheck = asList(
        ind("wx", "yz"),
        ind("w", "y"));

    assertThat(util.getMax(toCheck)).containsOnly(ind("wx", "yz"));
  }

  @Test
  void maxInd_sharedColumns_notContained() {
    final List<InclusionDependency> toCheck = asList(
        ind("wx", "yz"),
        ind("x", "y"));

    final List<InclusionDependency> expected = new ArrayList<>(toCheck);
    assertThat(util.getMax(toCheck)).hasSameElementsAs(expected);
  }

  @Test
  void maxInd_shift_notContained() {
    final List<InclusionDependency> toCheck = asList(
        ind("klm", "nop"),
        ind("lmk", "pno"));

    final List<InclusionDependency> expected = new ArrayList<>(toCheck);
    assertThat(util.getMax(toCheck)).hasSameElementsAs(expected);
  }

  @Test
  void maxInd_crossOver() {
    final List<InclusionDependency> toCheck = asList(
        ind("wx", "yz"),
        ind("xw", "zy"));

    // actually not defined which one will be retained
    assertThat(util.getMax(toCheck)).hasSize(1);
  }

  @Test
  void maxInd_ofDifferentTables() {
    final List<InclusionDependency> toCheck = asList(a_b(TABLE), a_b("other"));
    final List<InclusionDependency> expected = new ArrayList<>(toCheck);

    assertThat(util.getMax(toCheck)).hasSameElementsAs(expected);
  }

  @Test
  void maxInd_largestContainsAll() {
    final List<InclusionDependency> toCheck = asList(
        ind("abc", "def"),
        ind("a", "d"),
        ind("b", "e"),
        ind("c", "f"),
        ind("ab", "de"),
        ind("bc", "ef"));

    assertThat(util.getMax(toCheck)).containsOnly(ind("abc", "def"));
  }

  @Test
  void maxInd_emptyInput() {
    assertThat(util.getMax(emptyList())).isEmpty();
  }

  @Test
  void maxInd_trivialRetained() {
    final List<InclusionDependency> toCheck = asList(ind("a", "a"), ind("xy", "xy"));
    final List<InclusionDependency> expected = new ArrayList<>(toCheck);

    assertThat(util.getMax(toCheck)).hasSameElementsAs(expected);
  }

  @Test
  void maxInd_sortingProperty() {
    final List<InclusionDependency> toCheck = asList(ind("a", "b"), ind("xa", "xb"));

    assertThat(util.getMax(toCheck)).containsOnly(ind("xa", "xb"));
  }

  @Test
  void maxInd_withRepeatedColumns() {
    final List<InclusionDependency> toCheck = asList(ind("aaa", "bbb"), ind("aa", "bb"));

    assertThat(util.getMax(toCheck)).containsOnly(ind("aaa", "bbb"));
  }

  private InclusionDependency a_b(final String table) {
    return InclusionDependencyBuilder
        .dependent().column(table, "a")
        .referenced().column(table, "b")
        .build();
  }

  private InclusionDependency ind(final String lhs, final String rhs) {
    return InclusionDependencyBuilder
        .dependent().columns(TABLE, split(lhs))
        .referenced().columns(TABLE, split(rhs))
        .build();
  }

  private List<String> split(final String s) {
    final List<String> result = new ArrayList<>(s.length());
    for (char c : s.toCharArray()) {
      result.add(String.valueOf(c));
    }
    return result;
  }
}