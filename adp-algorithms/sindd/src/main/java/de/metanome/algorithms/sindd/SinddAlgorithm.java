package de.metanome.algorithms.sindd;

import static de.metanome.algorithms.sindd.ConfigurationKey.INPUT_ROW_LIMIT;
import static de.metanome.algorithms.sindd.ConfigurationKey.MAX_MEMORY_USAGE;
import static de.metanome.algorithms.sindd.ConfigurationKey.MEMORY_CHECK_INTERVAL;
import static de.metanome.algorithms.sindd.ConfigurationKey.OPEN_FILE_NR;
import static de.metanome.algorithms.sindd.ConfigurationKey.PARTITION_NR;
import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.ArrayList;
import java.util.List;

public class SinddAlgorithm implements
    InclusionDependencyAlgorithm,
    RelationalInputParameterAlgorithm,
    IntegerParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final Configuration defaultValues;
  private final Sindd sindd;

  public SinddAlgorithm() {
    builder = Configuration.builder();
    defaultValues = Configuration.withDefaults();
    sindd = new Sindd();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(relationalInput());
    requirements.addAll(tpmms());

    final ConfigurationRequirementInteger openFiles = new ConfigurationRequirementInteger(
        OPEN_FILE_NR.name());
    openFiles.setDefaultValues(new Integer[]{defaultValues.getOpenFileNr()});
    requirements.add(openFiles);

    final ConfigurationRequirementInteger partitions = new ConfigurationRequirementInteger(
        PARTITION_NR.name());
    partitions.setDefaultValues(new Integer[]{defaultValues.getPartitionNr()});
    requirements.add(partitions);

    return requirements;
  }

  private ConfigurationRequirement<?> relationalInput() {
    return new ConfigurationRequirementRelationalInput(
        ConfigurationKey.TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
  }

  private List<ConfigurationRequirement<?>> tpmms() {
    final ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(
        INPUT_ROW_LIMIT.name());
    inputRowLimit.setDefaultValues(new Integer[]{defaultValues.getInputRowLimit()});

    final ConfigurationRequirementInteger maxMemoryUsage = new ConfigurationRequirementInteger(
        MAX_MEMORY_USAGE.name());
    maxMemoryUsage.setDefaultValues(new Integer[]{defaultValues.getMaxMemoryUsage()});

    final ConfigurationRequirementInteger memoryCheckInterval = new ConfigurationRequirementInteger(
        MEMORY_CHECK_INTERVAL.name());
    memoryCheckInterval.setDefaultValues(new Integer[]{defaultValues.getMemoryCheckInterval()});

    return asList(inputRowLimit, maxMemoryUsage, memoryCheckInterval);
  }

  @Override
  public void setResultReceiver(final InclusionDependencyResultReceiver resultReceiver) {
    builder.resultReceiver(resultReceiver);
  }

  @Override
  public void setRelationalInputConfigurationValue(final String identifier,
      final RelationalInputGenerator... values) {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.relationalInputGenerators(asList(values));
    }
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values) {
    if (identifier.equals(ConfigurationKey.OPEN_FILE_NR.name())) {
      builder.openFileNr(values[0]);
    } else if (identifier.equals(ConfigurationKey.PARTITION_NR.name())) {
      builder.partitionNr(values[0]);
    } else if (identifier.equals(INPUT_ROW_LIMIT.name())) {
      builder.inputRowLimit(values[0]);
    } else if (identifier.equals(MAX_MEMORY_USAGE.name())) {
      builder.maxMemoryUsage(values[0]);
    } else if (identifier.equals(MEMORY_CHECK_INTERVAL.name())) {
      builder.memoryCheckInterval(values[0]);
    }
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