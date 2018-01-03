package de.metanome.input.ind;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.List;

public interface InclusionDependencyInput {

  List<InclusionDependency> execute() throws AlgorithmExecutionException;

}
