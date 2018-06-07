package de.metanome.util;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndDeduplicator {

    /**
     * Split up INDs with the same attributes on one side.
     * E.g. "ABC <= XXY" gets transformed to "AC <= XY" and "BC <= XY"
     */
    public static Set<InclusionDependency> deduplicateColumnIdentifier(InclusionDependency ind) {
        Set<InclusionDependency> inds = new HashSet<>();
        Set<Integer> dependantIndices = getDuplicateIndices(ind.getDependant().getColumnIdentifiers());
        Set<Integer> referencedIndices = getDuplicateIndices(ind.getReferenced().getColumnIdentifiers());
        if (dependantIndices.isEmpty() && referencedIndices.isEmpty()) {
            inds.add(ind);
        } else {
            inds.addAll(deduplicateColumnIdentifier(ind, dependantIndices));
            inds.addAll(deduplicateColumnIdentifier(ind, referencedIndices));
        }
        return inds;
    }

    private static Set<InclusionDependency> deduplicateColumnIdentifier(InclusionDependency ind, Set<Integer> duplicateIndices) {
        Set<InclusionDependency> inds = new HashSet<>();
        for (int index : duplicateIndices) {
            List<ColumnIdentifier> lhs = new ArrayList<>(ind.getDependant().getColumnIdentifiers());
            List<ColumnIdentifier> rhs = new ArrayList<>(ind.getReferenced().getColumnIdentifiers());
            lhs.remove(index);
            rhs.remove(index);
            ColumnPermutation dependant = new ColumnPermutation();
            dependant.setColumnIdentifiers(lhs);
            ColumnPermutation referenced = new ColumnPermutation();
            referenced.setColumnIdentifiers(rhs);
            inds.add(new InclusionDependency(dependant, referenced));
        }
        return inds;
    }

    private static Set<Integer> getDuplicateIndices(List<ColumnIdentifier> columns) {
        Set<Integer> indices = new HashSet<>();
        for (int i = 0; i < columns.size() - 1; i++) {
            for (int j = i + 1; j < columns.size(); j++) {
                if (columns.get(i).equals(columns.get(j))) {
                    indices.add(i);
                    indices.add(j);
                }
            }
        }
        return indices;
    }
}
