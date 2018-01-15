package de.metanome.algorithms.sindd;

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
import de.metanome.util.TPMMSConfiguration;
import de.metanome.util.TPMMSConfigurationRequirements;
import java.util.ArrayList;

public class SinddAlgorithm implements
    InclusionDependencyAlgorithm,
    RelationalInputParameterAlgorithm,
    IntegerParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final TPMMSConfiguration tpmmsConfiguration;
  private final Configuration defaultValues;
  private final Sindd sindd;

  public SinddAlgorithm() {
    builder = Configuration.builder();
    tpmmsConfiguration = new TPMMSConfiguration();
    defaultValues = Configuration.withDefaults();
    sindd = new Sindd();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(relationalInput());
    requirements.addAll(TPMMSConfigurationRequirements.tpmms());

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

    if (TPMMSConfigurationRequirements.acceptInteger(identifier, values, tpmmsConfiguration)) {
      return;
    }

    if (identifier.equals(ConfigurationKey.OPEN_FILE_NR.name())) {
      builder.openFileNr(values[0]);
    } else if (identifier.equals(ConfigurationKey.PARTITION_NR.name())) {
      builder.partitionNr(values[0]);
    }
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.tpmmsConfiguration(tpmmsConfiguration).build();
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