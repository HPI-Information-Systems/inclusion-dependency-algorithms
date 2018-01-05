package de.metanome.input.ind;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.spider.Spider;
import de.metanome.algorithms.spider.SpiderConfiguration;
import java.util.List;
import java.util.Optional;

class SpiderInput {

  List<InclusionDependency> execute(final InclusionDependencyParameters parameters)
      throws AlgorithmExecutionException {

    final SpiderConfiguration configuration = prepareConfiguration(parameters);

    final CollectingResultReceiver resultReceiver = new CollectingResultReceiver();
    configuration.setResultReceiver(resultReceiver);

    final Spider spider = new Spider();
    spider.execute(configuration);
    return resultReceiver.getReceived();
  }

  private SpiderConfiguration prepareConfiguration(final InclusionDependencyParameters parameters) {
    final SpiderConfiguration configuration = SpiderConfiguration.withDefaults();
    ConfigurationMapper.applyFrom(parameters.getConfigurationString(), configuration);
    configuration.setRelationalInputGenerators(
            Optional.ofNullable(parameters.getRelationalInputGenerators()).orElse(ImmutableList.of()));
    configuration.setTableInputGenerators(
            Optional.ofNullable(parameters.getTableInputGenerators()).orElse(ImmutableList.of()));
    return configuration;
  }
}