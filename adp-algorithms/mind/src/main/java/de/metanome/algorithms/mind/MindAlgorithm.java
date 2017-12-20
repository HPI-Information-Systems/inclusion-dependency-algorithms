package de.metanome.algorithms.mind;

import static de.metanome.algorithms.mind.ConfigurationKey.DATABASE;
import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.DatabaseConnectionParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementDatabaseConnection;
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
    DatabaseConnectionParameterAlgorithm,
    InclusionDependencyValidationAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final ValidationParameters validationParameters;

  public MindAlgorithm() {
    builder = Configuration.builder();
    validationParameters = new ValidationParameters();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
    requirements.addAll(ValidationConfigurationRequirements.validationStrategy());
    requirements.add(new ConfigurationRequirementDatabaseConnection(DATABASE.name()));
    return requirements;
  }

  private ConfigurationRequirement<?> tableInput() {
    return new ConfigurationRequirementTableInput(
        ConfigurationKey.TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
  }


  @Override
  public void setResultReceiver(final InclusionDependencyResultReceiver resultReceiver) {
    builder.resultReceiver(resultReceiver);
  }

  @Override
  public void setTableInputConfigurationValue(final String identifier,
      final TableInputGenerator... values) {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.tableInputGenerators(asList(values));
    }
  }

  @Override
  public void setDatabaseConnectionGeneratorConfigurationValue(final String identifier,
      final DatabaseConnectionGenerator... values) {
    if (identifier.equals(ConfigurationKey.DATABASE.name())) {
      builder.databaseConnectionGenerator(values[0]);
      ValidationConfigurationRequirements
          .acceptDatabaseConnectionGenerator(values, validationParameters);
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
}