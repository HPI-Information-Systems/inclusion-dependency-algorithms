package de.metanome.algorithms.bellbrockhausen.models;

import de.metanome.algorithm_integration.results.InclusionDependency;
import lombok.Data;

@Data
public class IndTest {
    private final InclusionDependency test;
    private boolean isDeleted = false;
}
