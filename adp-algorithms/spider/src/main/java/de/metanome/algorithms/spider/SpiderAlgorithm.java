package de.metanome.algorithms.spider;

import static de.metanome.algorithms.spider.ConfigurationKey.CLEAR_TEMPORARY_FOLDER;
import static de.metanome.algorithms.spider.ConfigurationKey.INPUT_ROW_LIMIT;
import static de.metanome.algorithms.spider.ConfigurationKey.MAX_MEMORY_USAGE;
import static de.metanome.algorithms.spider.ConfigurationKey.MEMORY_CHECK_INTERVAL;
import static de.metanome.algorithms.spider.ConfigurationKey.TEMPORARY_FOLDER_PATH;
import static java.util.Arrays.asList;

import com.google.common.base.Joiner;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.spider.SpiderConfiguration.SpiderConfigurationBuilder;
import java.util.ArrayList;
import java.util.List;

abstract class SpiderAlgorithm implements InclusionDependencyAlgorithm,
    StringParameterAlgorithm, BooleanParameterAlgorithm, IntegerParameterAlgorithm {

  final SpiderConfigurationBuilder builder;
  final Spider spider;

  SpiderAlgorithm() {
    builder = SpiderConfiguration.builder();
    spider = new Spider();
  }

  List<ConfigurationRequirement<?>> common() {
    final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.addAll(temporaryFolder());
    requirements.addAll(tpmms());
    return requirements;
  }

  private List<ConfigurationRequirement<?>> temporaryFolder() {
    final ConfigurationRequirementString temp = new ConfigurationRequirementString(
        TEMPORARY_FOLDER_PATH.name());
    temp.setRequired(false);

    final ConfigurationRequirementBoolean clearTemp = new ConfigurationRequirementBoolean(
        CLEAR_TEMPORARY_FOLDER.name());
    clearTemp.setDefaultValues(new Boolean[]{true});

    return asList(temp, clearTemp);
  }

  private List<ConfigurationRequirement<?>> tpmms() {
    final ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(
        INPUT_ROW_LIMIT.name());
    inputRowLimit.setDefaultValues(new Integer[]{-1});

    final ConfigurationRequirementInteger maxMemoryUsage = new ConfigurationRequirementInteger(
        MAX_MEMORY_USAGE.name());
    maxMemoryUsage.setDefaultValues(new Integer[]{2048 * 1024});

    final ConfigurationRequirementInteger memoryCheckInterval = new ConfigurationRequirementInteger(
        MEMORY_CHECK_INTERVAL.name());
    memoryCheckInterval.setDefaultValues(new Integer[]{500});

    return asList(inputRowLimit, maxMemoryUsage, memoryCheckInterval);
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
    final SpiderConfiguration configuration = builder.build();
    spider.execute(configuration);
  }

  @Override
  public void setResultReceiver(
      InclusionDependencyResultReceiver inclusionDependencyResultReceiver) {
    builder.resultReceiver(inclusionDependencyResultReceiver);
  }

  @Override
  public void setStringConfigurationValue(String identifier, String... values)
      throws AlgorithmConfigurationException {

    if (identifier.equals(TEMPORARY_FOLDER_PATH.name())) {
      builder.temporaryFolderPath(get(identifier, values, 0));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

  @Override
  public void setIntegerConfigurationValue(String identifier, Integer... values)
      throws AlgorithmConfigurationException {

    final int value = get(identifier, values, 0);
    if (identifier.equals(INPUT_ROW_LIMIT.name())) {
      builder.inputRowLimit(value);
    } else if (identifier.equals(MAX_MEMORY_USAGE.name())) {
      builder.maxMemoryUsage(value);
    } else if (identifier.equals(MEMORY_CHECK_INTERVAL.name())) {
      builder.memoryCheckInterval(value);
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

  @Override
  public void setBooleanConfigurationValue(String identifier, Boolean... values)
      throws AlgorithmConfigurationException {

    if (identifier.equals(CLEAR_TEMPORARY_FOLDER.name())) {
      builder.clearTemporaryFolder(get(identifier, values, 0));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
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
