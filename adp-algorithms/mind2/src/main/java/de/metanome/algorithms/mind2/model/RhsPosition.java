package de.metanome.algorithms.mind2.model;

import de.metanome.algorithm_integration.results.InclusionDependency;
import lombok.Data;

@Data
public class RhsPosition {

    private final InclusionDependency uind;
    private final Integer rhs;
}
