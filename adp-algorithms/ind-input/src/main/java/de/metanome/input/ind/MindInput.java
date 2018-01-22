package de.metanome.input.ind;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind.Mind;
import de.metanome.algorithms.mind.Configuration;

import java.util.List;
import java.util.Optional;

public class MindInput {

  List<InclusionDependency> execute(final InclusionDependencyParameters parameters)
      throws AlgorithmExecutionException {

    final Configuration configuration = prepareConfiguration(parameters);
    final CollectingResultReceiver resultReceiver = new CollectingResultReceiver();
    configuration.setResultReceiver(resultReceiver);

    final Mind mind = new Mind();
    mind.execute(configuration);
    return resultReceiver.getReceived();
  }

  private Configuration prepareConfiguration(final InclusionDependencyParameters parameters) {
    final Configuration configuration = Configuration.withDefaults();
    ConfigurationMapper.applyFrom(parameters.getConfigurationString(), configuration);
    configuration.setTableInputGenerators(
        Optional.ofNullable(parameters.getTableInputGenerators()).orElse(ImmutableList.of()));
    configuration.setMaxDepth(-1);
    return configuration;
  }
}
