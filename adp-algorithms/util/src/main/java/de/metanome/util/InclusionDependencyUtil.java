package de.metanome.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class InclusionDependencyUtil {

  /**
   * Sorts the attributes in an ind. Keeps persistent ordering between the RHS and the LHS attributes.
   */
  public static ImmutableSet<InclusionDependency> sortIndAttributes(Collection<InclusionDependency> inds) {
    return inds.stream()
            .map(InclusionDependencyUtil::sortAttributes)
            .collect(toImmutableSet());
  }

  private static InclusionDependency sortAttributes(InclusionDependency ind) {
    List<ColumnIdentifier> lhs = ind.getDependant().getColumnIdentifiers();
    List<ColumnIdentifier> rhs = ind.getReferenced().getColumnIdentifiers();
    List<List<ColumnIdentifier>> tuples = new ArrayList<>();
    for (int i = 0; i < lhs.size(); i++) {
      tuples.add(ImmutableList.of(lhs.get(i), rhs.get(i)));
    }
    tuples.sort(Comparator.comparing(elemsA -> elemsA.get(0)));
    List<ColumnIdentifier> sortedAttrA = new ArrayList<>();
    List<ColumnIdentifier> sortedAttrB = new ArrayList<>();
    for (List<ColumnIdentifier> elems: tuples) {
      sortedAttrA.add(elems.get(0));
      sortedAttrB.add(elems.get(1));
    }
    return new InclusionDependency(new ColumnPermutation(toArray(sortedAttrA)), new ColumnPermutation(toArray(sortedAttrB)));
  }

  private static ColumnIdentifier[] toArray(Collection<ColumnIdentifier> elems) {
    return elems.toArray(new ColumnIdentifier[0]);
  }

  /**
   * Retain only INDs which are not directly contained by other higher-arity INDs.
   *
   * <p>The basic idea is that all INDs are first sorted by degree in descending order.
   * Subsequently beginning with the IND of highest arity the list is scanned from left to right
   * in order to evict any contained INDs.</p>
   *
   * <p>The contains-check consists of checking if all LHS-columns appear in the LHS of the higher
   * arity IND and if so, the RHS columns must match given the position mapping from the LHS, too.
   * The transitivity property is currently not exploited.</p>
   */
  public List<InclusionDependency> getMax(final Collection<InclusionDependency> toMinify) {
    final List<InclusionDependency> ind = new LinkedList<>(toMinify);
    ind.sort(Comparator.comparing(this::degreeOf).reversed());

    final List<InclusionDependency> maxInd = new ArrayList<>();
    while (!ind.isEmpty()) {
      final InclusionDependency current = ind.remove(0);
      ind.removeAll(containedOf(current, ind));
      maxInd.add(current);
    }

    return maxInd;
  }

  private int degreeOf(final InclusionDependency ind) {
    return ind.getDependant().getColumnIdentifiers().size();
  }

  private List<InclusionDependency> containedOf(final InclusionDependency ind,
      final List<InclusionDependency> candidates) {

    final List<InclusionDependency> contained = new ArrayList<>();
    for (final InclusionDependency candidate : candidates) {
      if (isContained(candidate, ind)) {
        contained.add(candidate);
      }
    }
    return contained;
  }

  private boolean isContained(final InclusionDependency toCheck, final InclusionDependency parent) {
    Preconditions.checkState(degreeOf(toCheck) <= degreeOf(parent));

    final Int2IntMap mapping = new Int2IntOpenHashMap(degreeOf(toCheck));

    // for each column of the lower degree IND LHS
    for (int index = 0; index < degreeOf(toCheck); ++index) {
      final ColumnIdentifier x = toCheck.getDependant().getColumnIdentifiers().get(index);

      // check if the column exists in the higher degree IND LHS
      for (int k = 0; k < degreeOf(parent); ++k) {
        final ColumnIdentifier y = parent.getDependant().getColumnIdentifiers().get(k);
        if (x.equals(y)) {
          mapping.put(index, k);
          break;
        }
      }
    }

    // If not all LHS columns were found in the higher arity IND, early exit
    if (mapping.size() < degreeOf(toCheck)) {
      return false;
    }

    // Else the same mapping must hold for RHS
    for (final Int2IntMap.Entry entry : mapping.int2IntEntrySet()) {

      final ColumnIdentifier x = toCheck.getReferenced()
          .getColumnIdentifiers().get(entry.getIntKey());

      final ColumnIdentifier y = parent.getReferenced()
          .getColumnIdentifiers().get(entry.getIntValue());

      if (!x.equals(y)) {
        return false;
      }
    }

    return true;
  }
}
