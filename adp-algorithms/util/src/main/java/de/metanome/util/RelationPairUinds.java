package de.metanome.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.InclusionDependency;
import lombok.Data;

import java.util.Collection;
import java.util.Iterator;

import static de.metanome.util.Collectors.toImmutableList;
import static de.metanome.util.Collectors.toImmutableSet;
import static de.metanome.util.UindUtils.getDependant;
import static de.metanome.util.UindUtils.getReferenced;

/**
 * Groups UINDs by relation pairs. All UINDs in a group have the same dependant and referenced table.
 */
public class RelationPairUinds implements Iterable<ImmutableSet<InclusionDependency>> {

    @Data
    private class ColumnsKey {
        private final String referencedTable;
        private final String dependantTable;
    }

    private final SetMultimap<ColumnsKey, InclusionDependency> relationPairs = MultimapBuilder
            .hashKeys().hashSetValues().build();

    public RelationPairUinds(Collection<InclusionDependency> uinds) {
        filterUinds(uinds).forEach(uind -> {
            ColumnsKey columnsKey = new ColumnsKey(
                    getReferenced(uind).getTableIdentifier(),
                    getDependant(uind).getTableIdentifier());
            relationPairs.put(columnsKey, uind);
        });
    }

    @Override
    public Iterator<ImmutableSet<InclusionDependency>> iterator() {
        return relationPairs.keySet().stream()
                .map(key -> ImmutableSet.copyOf(relationPairs.get(key)))
                .collect(toImmutableList())
                .iterator();
    }

    private ImmutableSet<InclusionDependency> filterUinds(Collection<InclusionDependency> uinds) {
        return uinds.stream()
                .filter(uind -> {
                    ColumnIdentifier lhs = getDependant(uind);
                    ColumnIdentifier rhs = getReferenced(uind);
                    return !lhs.getTableIdentifier().equals(rhs.getTableIdentifier());
                })
                .collect(toImmutableSet());
    }
}
