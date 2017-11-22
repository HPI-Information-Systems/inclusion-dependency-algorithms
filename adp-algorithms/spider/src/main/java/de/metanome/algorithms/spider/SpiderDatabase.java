package de.metanome.algorithms.spider;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.ArrayList;

public class SpiderDatabase implements InclusionDependencyAlgorithm {

  @Override
  public void setResultReceiver(
      InclusionDependencyResultReceiver inclusionDependencyResultReceiver) {
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    return null;
  }

  @Override
  public void execute() throws AlgorithmExecutionException {

  }

  @Override
  public String getAuthors() {
    return "";
  }

  @Override
  public String getDescription() {
    return "";
  }
}
