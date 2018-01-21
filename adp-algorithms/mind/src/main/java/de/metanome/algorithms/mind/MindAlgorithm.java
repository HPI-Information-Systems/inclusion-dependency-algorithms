package de.metanome.algorithms.mind;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.validation.InclusionDependencyValidationAlgorithm;
import de.metanome.validation.ValidationConfigurationRequirements;
import de.metanome.validation.ValidationParameters;
import java.util.ArrayList;

public class MindAlgorithm implements InclusionDependencyAlgorithm,
    TableInputParameterAlgorithm,
    InclusionDependencyValidationAlgorithm,
    IntegerParameterAlgorithm,
    BooleanParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final ValidationParameters validationParameters;
  private final Configuration defaultValues;

  public MindAlgorithm() {
    builder = Configuration.builder();
    defaultValues = Configuration.withDefaults();
    validationParameters = new ValidationParameters();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
    requirements.addAll(ValidationConfigurationRequirements.validationStrategy());
    requirements.add(maxDepth());
    requirements.add(processEmptyColumns());
    return requirements;
  }

  private ConfigurationRequirement<?> tableInput() {
    return new ConfigurationRequirementTableInput(
        ConfigurationKey.TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
  }

  private ConfigurationRequirement<?> maxDepth() {
    final ConfigurationRequirementInteger requirement = new ConfigurationRequirementInteger(
        ConfigurationKey.MAX_DEPTH.name());
    requirement.setDefaultValues(new Integer[]{defaultValues.getMaxDepth()});
    return requirement;
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
  public void setTableInputConfigurationValue(final String identifier,
      final TableInputGenerator... values) {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      final DatabaseConnectionGenerator databaseConnectionGenerators = values[0]
          .getDatabaseConnectionGenerator();
      ValidationConfigurationRequirements
          .acceptDatabaseConnectionGenerator(databaseConnectionGenerators, validationParameters);
      builder.tableInputGenerators(asList(values));
    }
  }

  @Override
  public void setListBoxConfigurationValue(final String identifier,
      final String... selectedValues) {

    ValidationConfigurationRequirements
        .acceptListBox(identifier, selectedValues, validationParameters);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.validationParameters(validationParameters).build();

    final Mind mind = new Mind();
    mind.execute(configuration);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch, Tim Friedrich";
  }

  @Override
  public String getDescription() {
    return "MIND";
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values) {
    if (identifier.equals(ConfigurationKey.MAX_DEPTH.name())) {
      builder.maxDepth(values[0]);
    }
  }

  @Override
  public void setBooleanConfigurationValue(final String identifier, final Boolean... values) {
    if (identifier.equals(ConfigurationKey.PROCESS_EMPTY_COLUMNS.name())) {
      builder.processEmptyColumns(values[0]);
    }
  }
}