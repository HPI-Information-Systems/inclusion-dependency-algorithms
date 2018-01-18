package de.metanome.algorithms.find2;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
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
import de.metanome.validation.InclusionDependencyValidationAlgorithm;
import de.metanome.validation.ValidationConfigurationRequirements;
import de.metanome.validation.ValidationParameters;
import java.util.ArrayList;

public class FIND2Algorithm
    implements InclusionDependencyAlgorithm,
        IntegerParameterAlgorithm,
        TableInputParameterAlgorithm,
        DatabaseConnectionParameterAlgorithm,
        InclusionDependencyValidationAlgorithm {

  private static final String START_K = "START_K";
  private final FIND2Configuration.FIND2ConfigurationBuilder configurationBuilder;
  private final ValidationParameters validationParameters;

  public FIND2Algorithm() {
    configurationBuilder = FIND2Configuration.builder();
    validationParameters = new ValidationParameters();
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    configurationBuilder.validationParameters(validationParameters);
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
    ConfigurationRequirementInteger startKConfig = new ConfigurationRequirementInteger(START_K);
    startKConfig.setDefaultValues(new Integer[] {2});
    reqs.add(startKConfig);
    reqs.add(new ConfigurationRequirementDatabaseConnection(ConfigurationKey.DATABASE.name()));
    return reqs;
  }

  @Override
  public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
    configurationBuilder.resultReceiver(resultReceiver);
  }

  @Override
  public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values)
      throws AlgorithmConfigurationException {
    if (identifier.equals(ConfigurationKey.TABLE.name()) && values.length > 0) {
      configurationBuilder.tableInputGenerators(asList(values));
    }
  }

  @Override
  public void setDatabaseConnectionGeneratorConfigurationValue(
      String identifier, DatabaseConnectionGenerator... values)
      throws AlgorithmConfigurationException {
    if (identifier.equals(ConfigurationKey.DATABASE.name()) && values.length > 0) {
      configurationBuilder.databaseConnectionGenerator(values[0]);
      ValidationConfigurationRequirements.acceptDatabaseConnectionGenerator(
          values, validationParameters);
    }
  }

  @Override
  public void setListBoxConfigurationValue(String identifier, String... selectedValues)
      throws AlgorithmConfigurationException {
    ValidationConfigurationRequirements.acceptListBox(
        identifier, selectedValues, validationParameters);
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
  public void setIntegerConfigurationValue(String identifier, Integer... values)
      throws AlgorithmConfigurationException {
    if (identifier.equals(START_K) && values.length > 0) {
      configurationBuilder.startK(values[0]);
    }
  }
}
