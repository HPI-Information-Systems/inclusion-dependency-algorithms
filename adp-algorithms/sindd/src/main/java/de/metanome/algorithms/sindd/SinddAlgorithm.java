package de.metanome.algorithms.sindd;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.ArrayList;

public class SinddAlgorithm implements InclusionDependencyAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final Sindd sindd;

  public SinddAlgorithm() {
    builder = Configuration.builder();
    sindd = new Sindd();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    return requirements;
  }

  @Override
  public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
    builder.resultReceiver(resultReceiver);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.build();
    sindd.execute(configuration);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch, Tim Friedrich";
  }

  @Override
  public String getDescription() {
    return "S-INDD";
  }
}