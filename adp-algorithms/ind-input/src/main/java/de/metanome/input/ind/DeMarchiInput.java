package de.metanome.input.ind;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.demarchi.Configuration;
import de.metanome.algorithms.demarchi.DeMarchi;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class DeMarchiInput implements InclusionDependencyInput {

  private final InclusionDependencyParameters parameters;

  @Override
  public List<InclusionDependency> execute() throws AlgorithmExecutionException {

    final Configuration configuration = prepareConfiguration();
    final CollectingResultReceiver resultReceiver = new CollectingResultReceiver();
    configuration.setResultReceiver(resultReceiver);

    final DeMarchi deMarchi = new DeMarchi();
    deMarchi.execute(configuration);

    return resultReceiver.getReceived();
  }

  private Configuration prepareConfiguration() {
    final Configuration configuration = Configuration.withDefaults();
    configuration.setProcessEmptyColumns(false);
    ConfigurationMapper.applyFrom(parameters.getConfigurationString(), configuration);

    if (parameters.getRelationalInputGenerators() != null) {
      configuration.setRelationalInputGenerators(parameters.getRelationalInputGenerators());
    }

    if (parameters.getTableInputGenerators() != null) {
      configuration.setTableInputGenerators(parameters.getTableInputGenerators());
    }

    return configuration;
  }
}
