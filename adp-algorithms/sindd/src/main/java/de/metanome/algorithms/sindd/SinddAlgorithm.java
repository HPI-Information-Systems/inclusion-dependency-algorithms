package de.metanome.algorithms.sindd;

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

import java.util.ArrayList;

import static java.util.Arrays.asList;

public class SinddAlgorithm implements InclusionDependencyAlgorithm,
    TableInputParameterAlgorithm, DatabaseConnectionParameterAlgorithm, IntegerParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final Sindd sindd;

  public SinddAlgorithm() {
    builder = Configuration.builder();
    sindd = new Sindd();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
    requirements.add(new ConfigurationRequirementDatabaseConnection(
        ConfigurationKey.DATABASE_IDENTIFIER.name()));

    requirements.add(new ConfigurationRequirementInteger(ConfigurationKey.OPEN_FILE_NR.name()));
    requirements.add(new ConfigurationRequirementInteger(ConfigurationKey.PARTITION_NR.name()));

    return requirements;
  }

  private ConfigurationRequirement<?> tableInput() {
    return new ConfigurationRequirementTableInput(
        ConfigurationKey.TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES);
  }

  @Override
  public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
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
  public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
    if (identifier.equals(ConfigurationKey.OPEN_FILE_NR.name())) {
      builder.openFileNr(values[0]);
    }
    if (identifier.equals(ConfigurationKey.PARTITION_NR.name())) {
      builder.partitionNr(values[0]);
    }
  }

  @Override
  public void setDatabaseConnectionGeneratorConfigurationValue(final String identifier,
                                                               final DatabaseConnectionGenerator... values) {

    if (identifier.equals(ConfigurationKey.DATABASE_IDENTIFIER.name())) {
      builder.databaseConnectionGenerator(values[0]);
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