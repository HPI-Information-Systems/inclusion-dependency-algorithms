package de.metanome.util;

import com.google.common.base.Preconditions;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.InclusionDependency;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class InclusionDependencyUtil {

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
