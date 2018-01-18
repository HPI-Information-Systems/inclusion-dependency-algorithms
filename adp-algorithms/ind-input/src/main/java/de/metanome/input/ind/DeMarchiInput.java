package de.metanome.input.ind;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.demarchi.Configuration;
import de.metanome.algorithms.demarchi.DeMarchi;
import java.util.List;
import java.util.Optional;

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
    configuration.setProcessEmptyColumns(false);
    ConfigurationMapper.applyFrom(parameters.getConfigurationString(), configuration);
    configuration.setRelationalInputGenerators(
        Optional.ofNullable(parameters.getRelationalInputGenerators()).orElse(ImmutableList.of()));
    configuration.setTableInputGenerators(
        Optional.ofNullable(parameters.getTableInputGenerators()).orElse(ImmutableList.of()));
    return configuration;
  }
}
