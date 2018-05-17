package de.metanome.algorithms.spiderbruteforce;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TempFileAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.util.TPMMSConfiguration;
import de.metanome.util.TPMMSConfigurationRequirements;
import java.util.ArrayList;
import java.util.List;

public class SpiderBruteForceAlgorithm implements InclusionDependencyAlgorithm,
    RelationalInputParameterAlgorithm,
    TempFileAlgorithm,
    BooleanParameterAlgorithm,
    IntegerParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final TPMMSConfiguration tpmmsConfiguration;
  private final Configuration defaultValues;
  private final SpiderBruteForce impl;

  public SpiderBruteForceAlgorithm() {
    builder = Configuration.builder();
    tpmmsConfiguration = new TPMMSConfiguration();
    defaultValues = Configuration.withDefaults();
    impl = new SpiderBruteForce();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(relationalInput());
    requirements.add(processEmptyColumns());
    requirements.addAll(TPMMSConfigurationRequirements.tpmms());
    return new ArrayList<>(requirements);
  }

  private ConfigurationRequirement<?> relationalInput() {
    return new ConfigurationRequirementRelationalInput(
        ConfigurationKey.TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
  }

  private ConfigurationRequirement<?> processEmptyColumns() {
    final ConfigurationRequirementBoolean requirement = new ConfigurationRequirementBoolean(
        ConfigurationKey.PROCESS_EMPTY_COLUMNS.name());
    requirement.setDefaultValues(new Boolean[]{defaultValues.isProcessEmptyColumns()});
    return requirement;
  }

  @Override
  public void setResultReceiver(final InclusionDependencyResultReceiver resultReceiver) {
    builder.resultReceiver(resultReceiver);
  }

  @Override
  public void setTempFileGenerator(final FileGenerator tempFileGenerator) {
    builder.tempFileGenerator(tempFileGenerator);
  }


  @Override
  public void setRelationalInputConfigurationValue(final String identifier,
      final RelationalInputGenerator... values) {

    if (ConfigurationKey.TABLE.name().equals(identifier)) {
      builder.relationalInputGenerators(asList(values));
    } else {
      handleUnknownConfiguration(identifier);
    }
  }

  @Override
  public void setBooleanConfigurationValue(final String identifier, final Boolean... values) {
    if (ConfigurationKey.PROCESS_EMPTY_COLUMNS.name().equals(identifier)) {
      builder.processEmptyColumns(values[0]);
    } else {
      handleUnknownConfiguration(identifier);
    }
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values) {
    TPMMSConfigurationRequirements.acceptInteger(identifier, values, tpmmsConfiguration);
  }

  private void handleUnknownConfiguration(final String identifier) {
    throw new IllegalArgumentException("Unknown identifier: " + identifier);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.tpmmsConfiguration(tpmmsConfiguration).build();
    impl.execute(configuration);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch";
  }

  @Override
  public String getDescription() {
    return "SPIDER Brute-Force";
  }
}
