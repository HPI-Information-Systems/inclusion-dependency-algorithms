package de.metanome.algorithms.mind;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.input.ind.InclusionDependencyInputConfigurationRequirements;
import de.metanome.input.ind.InclusionDependencyInputParameterAlgorithm;
import de.metanome.input.ind.InclusionDependencyParameters;
import de.metanome.validation.InclusionDependencyValidationAlgorithm;
import de.metanome.validation.ValidationConfigurationRequirements;
import de.metanome.validation.ValidationParameters;
import java.util.ArrayList;

public class MindAlgorithm implements InclusionDependencyAlgorithm,
    TableInputParameterAlgorithm,
    InclusionDependencyInputParameterAlgorithm,
    InclusionDependencyValidationAlgorithm,
    IntegerParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final InclusionDependencyParameters inclusionDependencyParameters;
  private final ValidationParameters validationParameters;
  private final Configuration defaultValues;

  public MindAlgorithm() {
    builder = Configuration.builder();
    defaultValues = Configuration.withDefaults();
    inclusionDependencyParameters = new InclusionDependencyParameters();
    validationParameters = new ValidationParameters();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
    requirements.addAll(InclusionDependencyInputConfigurationRequirements.indInput());
    requirements.addAll(ValidationConfigurationRequirements.validationStrategy());
    requirements.add(maxDepth());
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
      InclusionDependencyInputConfigurationRequirements
          .acceptTableInputGenerator(values, inclusionDependencyParameters);
      ValidationConfigurationRequirements
          .acceptDatabaseConnectionGenerator(databaseConnectionGenerators, validationParameters);
      builder.tableInputGenerators(asList(values));
    }
  }

  @Override
  public void setListBoxConfigurationValue(final String identifier,
      final String... selectedValues) {

    InclusionDependencyInputConfigurationRequirements
        .acceptListBox(identifier, selectedValues, inclusionDependencyParameters);
    ValidationConfigurationRequirements
        .acceptListBox(identifier, selectedValues, validationParameters);
  }

  @Override
  public void setStringConfigurationValue(final String identifier, final String... values) {

    InclusionDependencyInputConfigurationRequirements
        .acceptString(identifier, values, inclusionDependencyParameters);
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values) {
    if (identifier.equals(ConfigurationKey.MAX_DEPTH.name())) {
      builder.maxDepth(values[0]);
    }
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder
        .inclusionDependencyParameters(inclusionDependencyParameters)
        .validationParameters(validationParameters)
        .build();

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
}