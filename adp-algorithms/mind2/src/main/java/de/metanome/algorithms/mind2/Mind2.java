package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;

import javax.inject.Inject;

public class Mind2 {

    private final Mind2Configuration configuration;

    @Inject
    public Mind2(Mind2Configuration configuration) {
        this.configuration = configuration;
    }

    public void execute() throws AlgorithmExecutionException {
    }
}
