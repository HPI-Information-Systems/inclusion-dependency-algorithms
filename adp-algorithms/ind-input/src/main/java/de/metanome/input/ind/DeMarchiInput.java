package de.metanome.input.ind;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.demarchi.Configuration;
import de.metanome.algorithms.demarchi.DeMarchi;
import java.util.List;

class DeMarchiInput {

  List<InclusionDependency> execute(final InclusionDependencyParameters parameters)
      throws AlgorithmExecutionException {

    final Configuration configuration = prepareConfiguration(parameters);
    final CollectingResultReceiver resultReceiver = new CollectingResultReceiver();
    configuration.setResultReceiver(resultReceiver);

    final DeMarchi deMarchi = new DeMarchi();
    deMarchi.execute(configuration);

    return resultReceiver.getReceived();
  }

  private Configuration prepareConfiguration(final InclusionDependencyParameters parameters) {
    final Configuration configuration = Configuration.withDefaults();
    ConfigurationMapper.applyFrom(parameters.getConfigurationString(), configuration);
    configuration.setRelationalInputGenerators(parameters.getRelationalInputGenerators());
    configuration.setTableInputGenerators(parameters.getTableInputGenerators());
    return configuration;
  }
}
