package de.metanome.util;

import static de.metanome.util.Collectors.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.Set;
import org.junit.jupiter.api.Test;

class IndDeduplicatorTest {

  @Test
  void testDeduplication() {
    // GIVEN
    ColumnIdentifier a1 = new ColumnIdentifier("R", "A1");
    ColumnIdentifier a2 = new ColumnIdentifier("R", "A2");
    ColumnIdentifier a3 = new ColumnIdentifier("R", "A3");
    ColumnIdentifier b1 = new ColumnIdentifier("S", "B1");
    ColumnIdentifier b2 = new ColumnIdentifier("S", "B1");
    ColumnIdentifier b3 = new ColumnIdentifier("S", "B3");
    InclusionDependency ind = new InclusionDependency(
        new ColumnPermutation(a1, a2, a3),
        new ColumnPermutation(b1, b2, b3));
    Set<InclusionDependency> expectedInds = ImmutableSet.of(
        new InclusionDependency(new ColumnPermutation(a1, a3), new ColumnPermutation(b1, b3)),
        new InclusionDependency(new ColumnPermutation(a2, a3), new ColumnPermutation(b2, b3)));

    // WHEN
    Set<InclusionDependency> dedupedInds = IndDeduplicator.deduplicateColumnIdentifier(ind);

    // THEN
    assertThat(dedupedInds).isEqualTo(expectedInds);
  }

  @Test
  void test4ary() {
    // GIVEN
    ColumnIdentifier c1 = new ColumnIdentifier("c", "1");
    ColumnIdentifier c4 = new ColumnIdentifier("c", "4");
    ColumnIdentifier c5 = new ColumnIdentifier("c", "5");
    ColumnIdentifier c12 = new ColumnIdentifier("c", "12");
    ColumnIdentifier d1 = new ColumnIdentifier("d", "1");
    ColumnIdentifier d3 = new ColumnIdentifier("d", "3");
    ColumnIdentifier d4 = new ColumnIdentifier("d", "4");
    InclusionDependency ind = new InclusionDependency(new ColumnPermutation(c4, c12, c1, c5),
        new ColumnPermutation(d3, d1, d4, d1));
    Set<InclusionDependency> expectedInds = ImmutableSet.of(
        new InclusionDependency(new ColumnPermutation(c4, c1, c5),
            new ColumnPermutation(d3, d4, d1)),
        new InclusionDependency(new ColumnPermutation(c4, c12, c1),
            new ColumnPermutation(d3, d1, d4)));

    // WHEN
    Set<InclusionDependency> dedupedInds = IndDeduplicator.deduplicateColumnIdentifier(ind);

    // THEN
    assertThat(dedupedInds).isEqualTo(expectedInds);
  }

  @Test
  void testScopExample() {
    // GIVEN
    ColumnIdentifier c1 = new ColumnIdentifier("c", "1");
    ColumnIdentifier c4 = new ColumnIdentifier("c", "4");
    ColumnIdentifier c5 = new ColumnIdentifier("c", "5");
    ColumnIdentifier c9 = new ColumnIdentifier("c", "9");
    ColumnIdentifier c10 = new ColumnIdentifier("c", "10");
    ColumnIdentifier c11 = new ColumnIdentifier("c", "11");
    ColumnIdentifier c12 = new ColumnIdentifier("c", "12");
    ColumnIdentifier d1 = new ColumnIdentifier("d", "1");
    ColumnIdentifier d3 = new ColumnIdentifier("d", "3");
    ColumnIdentifier d4 = new ColumnIdentifier("d", "4");
    Set<InclusionDependency> inds = ImmutableSet.of(
        new InclusionDependency(new ColumnPermutation(c10, c4), new ColumnPermutation(d1, d3)),
        new InclusionDependency(new ColumnPermutation(c11, c4), new ColumnPermutation(d1, d3)),
        new InclusionDependency(new ColumnPermutation(c4, c9), new ColumnPermutation(d3, d1)),
        new InclusionDependency(new ColumnPermutation(c4, c12, c1, c5),
            new ColumnPermutation(d3, d1, d4, d1)));
    Set<InclusionDependency> expectedInds = ImmutableSet.of(
        new InclusionDependency(new ColumnPermutation(c10, c4), new ColumnPermutation(d1, d3)),
        new InclusionDependency(new ColumnPermutation(c11, c4), new ColumnPermutation(d1, d3)),
        new InclusionDependency(new ColumnPermutation(c4, c9), new ColumnPermutation(d3, d1)),
        new InclusionDependency(new ColumnPermutation(c4, c1, c5),
            new ColumnPermutation(d3, d4, d1)),
        new InclusionDependency(new ColumnPermutation(c4, c12, c1),
            new ColumnPermutation(d3, d1, d4)));

    // WHEN
    Set<InclusionDependency> dedupedInds = inds.stream()
        .map(IndDeduplicator::deduplicateColumnIdentifier)
        .flatMap(Set::stream)
        .collect(toImmutableSet());

    // THEN
    assertThat(dedupedInds).isEqualTo(expectedInds);
  }
}
