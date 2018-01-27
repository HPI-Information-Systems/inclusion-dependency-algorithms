package de.metanome.input.ind;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.spider.Spider;
import de.metanome.algorithms.spider.SpiderConfiguration;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class SpiderInput implements InclusionDependencyInput {

  private final InclusionDependencyParameters parameters;

  @Override
  public List<InclusionDependency> execute() throws AlgorithmExecutionException {
    final SpiderConfiguration configuration = prepareConfiguration();

    final CollectingResultReceiver resultReceiver = new CollectingResultReceiver();
    configuration.setResultReceiver(resultReceiver);

    final Spider spider = new Spider();
    spider.execute(configuration);
    return resultReceiver.getReceived();
  }

  private SpiderConfiguration prepareConfiguration() {
    final SpiderConfiguration configuration = SpiderConfiguration.withDefaults();
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