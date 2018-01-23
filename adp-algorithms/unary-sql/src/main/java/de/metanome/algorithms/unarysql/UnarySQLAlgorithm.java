package de.metanome.algorithms.unarysql;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.validation.InclusionDependencyValidationAlgorithm;
import de.metanome.validation.ValidationConfigurationRequirements;
import de.metanome.validation.ValidationParameters;
import java.util.ArrayList;

public class UnarySQLAlgorithm implements
    InclusionDependencyAlgorithm,
    TableInputParameterAlgorithm,
    InclusionDependencyValidationAlgorithm,
    BooleanParameterAlgorithm {

  private final Configuration defaultValues;
  private final Configuration.ConfigurationBuilder builder;
  private final ValidationParameters validationParameters;

  public UnarySQLAlgorithm() {
    defaultValues = Configuration.withDefaults();
    builder = Configuration.builder();
    validationParameters = new ValidationParameters();
  }


  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
    requirements.add(processEmptyColumns());
    requirements.addAll(ValidationConfigurationRequirements.validationStrategy());
    return requirements;
  }

  private ConfigurationRequirement<?> tableInput() {
    return new ConfigurationRequirementTableInput(ConfigurationKey.TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
  }

  private ConfigurationRequirement<?> processEmptyColumns() {
    final ConfigurationRequirementBoolean requirement = new ConfigurationRequirementBoolean(
        ConfigurationKey.PROCESS_EMPTY_COLUMNS.name());
    requirement.setDefaultValues(new Boolean[]{defaultValues.isProcessEmptyColumns()});
    return requirement;
  }

  @Override
  public void setBooleanConfigurationValue(final String identifier, final Boolean... values) {
    if (identifier.equals(ConfigurationKey.PROCESS_EMPTY_COLUMNS.name())) {
      builder.processEmptyColumns(values[0]);
    }
  }

  @Override
  public void setTableInputConfigurationValue(final String identifier,
      final TableInputGenerator... values) {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.tableInputGenerators(asList(values));

      ValidationConfigurationRequirements
          .acceptDatabaseConnectionGenerator(values[0].getDatabaseConnectionGenerator(),
              validationParameters);
    }
  }

  @Override
  public void setListBoxConfigurationValue(final String identifier,
      final String... selectedValues) {

    ValidationConfigurationRequirements
        .acceptListBox(identifier, selectedValues, validationParameters);
  }

  @Override
  public void setResultReceiver(final InclusionDependencyResultReceiver resultReceiver) {
    builder.resultReceiver(resultReceiver);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.validationParameters(validationParameters).build();

    final UnarySQL unarySQL = new UnarySQL();
    unarySQL.execute(configuration);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch, Tim Friedrich";
  }

  @Override
  public String getDescription() {
    return "Discover unary INDs through SQL";
  }
}
