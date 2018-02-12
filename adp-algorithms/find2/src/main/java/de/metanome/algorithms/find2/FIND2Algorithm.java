package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.DatabaseConnectionParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementDatabaseConnection;
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

public class FIND2Algorithm
    implements InclusionDependencyAlgorithm,
    IntegerParameterAlgorithm,
    TableInputParameterAlgorithm,
    DatabaseConnectionParameterAlgorithm,
    InclusionDependencyValidationAlgorithm,
    InclusionDependencyInputParameterAlgorithm {

  private final FIND2Configuration.FIND2ConfigurationBuilder configurationBuilder;
  private final ValidationParameters validationParameters;
  private final InclusionDependencyParameters inclusionDependencyParameters;

  public FIND2Algorithm() {
    configurationBuilder = FIND2Configuration.builder();
    validationParameters = new ValidationParameters();
    inclusionDependencyParameters = new InclusionDependencyParameters();
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    configurationBuilder.validationParameters(validationParameters)
        .inclusionDependencyParameters(inclusionDependencyParameters);
    FIND2 find2 = new FIND2(configurationBuilder.build());
    find2.execute();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    ArrayList<ConfigurationRequirement<?>> reqs = new ArrayList<>();
    reqs.add(
        new ConfigurationRequirementTableInput(
            ConfigurationKey.TABLE.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
    reqs.addAll(ValidationConfigurationRequirements.validationStrategy());
    reqs.addAll(InclusionDependencyInputConfigurationRequirements.indInput());
    ConfigurationRequirementInteger startKConfig = new ConfigurationRequirementInteger(
        ConfigurationKey.START_K.name());
    startKConfig.setDefaultValues(new Integer[]{2});
    reqs.add(startKConfig);
    reqs.add(new ConfigurationRequirementDatabaseConnection(ConfigurationKey.DATABASE.name()));
    return reqs;
  }

  @Override
  public void setResultReceiver(final InclusionDependencyResultReceiver resultReceiver) {
    configurationBuilder.resultReceiver(resultReceiver);
  }

  @Override
  public void setTableInputConfigurationValue(final String identifier,
      final TableInputGenerator... values) {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      InclusionDependencyInputConfigurationRequirements
          .acceptTableInputGenerator(values, inclusionDependencyParameters);
    }
  }

  @Override
  public void setDatabaseConnectionGeneratorConfigurationValue(
      final String identifier, final DatabaseConnectionGenerator... values) {

    if (identifier.equals(ConfigurationKey.DATABASE.name())) {
      ValidationConfigurationRequirements.acceptDatabaseConnectionGenerator(
          values[0], validationParameters);
    }
  }

  @Override
  public void setListBoxConfigurationValue(final String identifier,
      final String... selectedValues) {

    if (ValidationConfigurationRequirements.acceptListBox(
        identifier, selectedValues, validationParameters)) {
      return;
    }

    InclusionDependencyInputConfigurationRequirements
        .acceptListBox(identifier, selectedValues, inclusionDependencyParameters);
  }

  @Override
  public void setStringConfigurationValue(final String identifier, final String... values) {

    InclusionDependencyInputConfigurationRequirements
        .acceptString(identifier, values, inclusionDependencyParameters);
  }

  @Override
  public String getAuthors() {
    return "Paper: Andreas Koeller, Elke Rundensteiner; Implementation: Axel Stebner, Maxi Fischer";
  }

  @Override
  public String getDescription() {
    return "Discovery of High Dimensional Inclusion Dependencies";
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values) {
    if (identifier.equals(ConfigurationKey.START_K.name()) && values.length > 0) {
      configurationBuilder.startK(values[0]);
    }
  }
}
