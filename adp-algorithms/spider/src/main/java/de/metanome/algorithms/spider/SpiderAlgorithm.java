package de.metanome.algorithms.spider;

import com.google.common.base.Joiner;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TempFileAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.spider.SpiderConfiguration.SpiderConfigurationBuilder;
import de.metanome.util.TPMMSConfiguration;
import de.metanome.util.TPMMSConfigurationRequirements;
import java.util.ArrayList;
import java.util.List;

abstract class SpiderAlgorithm implements InclusionDependencyAlgorithm,
    IntegerParameterAlgorithm,
    TempFileAlgorithm {

  final SpiderConfigurationBuilder builder;
  final TPMMSConfiguration tpmmsConfiguration;
  final SpiderConfiguration defaultValues;
  final Spider spider;

  SpiderAlgorithm() {
    builder = SpiderConfiguration.builder();
    tpmmsConfiguration = new TPMMSConfiguration();
    defaultValues = SpiderConfiguration.withDefaults();
    spider = new Spider();
  }

  List<ConfigurationRequirement<?>> common() {
    final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.addAll(TPMMSConfigurationRequirements.tpmms());
    return requirements;
  }

  <T> T get(final String identifier, final T[] values, final int index)
      throws AlgorithmConfigurationException {

    if (index >= values.length) {
      final String message = String
          .format("Expected at least %d items width identifier %s", index + 1, identifier);
      throw new AlgorithmConfigurationException(message);
    }
    return values[index];
  }

  @SafeVarargs
  final <T> void handleUnknownConfiguration(final String identifier, final T... values)
      throws AlgorithmConfigurationException {

    final String formattedValues = Joiner.on(", ").join(values);
    final String message = String
        .format("Unknown configuration '%s', values: '%s'", identifier, formattedValues);
    throw new AlgorithmConfigurationException(message);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final SpiderConfiguration configuration = builder.tpmmsConfiguration(tpmmsConfiguration)
        .build();
    spider.execute(configuration);
  }

  @Override
  public void setResultReceiver(
      InclusionDependencyResultReceiver inclusionDependencyResultReceiver) {
    builder.resultReceiver(inclusionDependencyResultReceiver);
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values)
      throws AlgorithmConfigurationException {

    if (TPMMSConfigurationRequirements.acceptInteger(identifier, values, tpmmsConfiguration)) {
      return;
    }

    handleUnknownConfiguration(identifier, values);
  }

  @Override
  public void setTempFileGenerator(final FileGenerator tempFileGenerator) {
    builder.tempFileGenerator(tempFileGenerator);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch, Tim Friedrich";
  }

  @Override
  public String getDescription() {
    return "Sort-Merge-Join IND discovery";
  }
}
