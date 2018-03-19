package de.metanome.algorithms.bellbrockhausen.models;

import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class IndTest {

    public static IndTest fromAttributes(Attribute dependant, Attribute referenced) {
        InclusionDependency ind = new InclusionDependency(
                new ColumnPermutation(dependant.getColumnIdentifier()),
                new ColumnPermutation(referenced.getColumnIdentifier()));
        return new IndTest(ind, dependant, referenced);
    }

    private final InclusionDependency ind;
    private final Attribute dependant;
    private final Attribute referenced;
    private boolean isDeleted = false;

    public void delete() {
        isDeleted = true;
    }
}
