package de.metanome.algorithms.bellbrockhausen.models;

import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class IndTest {
    private final InclusionDependency test;
    private boolean isDeleted;

    public static IndTest fromAttributes(Attribute dependant, Attribute referenced) {
        boolean hasSameType = dependant.getDataType().equals(referenced.getDataType());
        boolean isDeleted = !hasSameType || !referenced.getValueRange().encloses(dependant.getValueRange());
        InclusionDependency ind = new InclusionDependency(
                new ColumnPermutation(dependant.getColumnIdentifier()),
                new ColumnPermutation(referenced.getColumnIdentifier()));
        return new IndTest(ind, isDeleted);
    }
}
