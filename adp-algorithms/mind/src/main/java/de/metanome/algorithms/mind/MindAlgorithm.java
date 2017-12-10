package de.metanome.algorithms.mind;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.algorithm_types.DatabaseConnectionParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementDatabaseConnection;


import java.util.ArrayList;


import java.util.ArrayList;

public class MindAlgorithm implements InclusionDependencyAlgorithm,
        TableInputParameterAlgorithm, DatabaseConnectionParameterAlgorithm {

  private final Configuration.ConfigurationBuilder builder;
  private final Mind mind;
  protected DatabaseConnectionGenerator databaseConnectionGenerator;
  public static final String DATABASE_IDENTIFIER = "database identifier";


  public MindAlgorithm() {
    builder = Configuration.builder();
    mind = new Mind();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
    requirements.add(new ConfigurationRequirementDatabaseConnection(
        DATABASE_IDENTIFIER));
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
  public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values)
          throws AlgorithmConfigurationException {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.tableInputGenerators(asList(values));
    }
  }

  @Override
  public void setDatabaseConnectionGeneratorConfigurationValue(String identifier,
                                                        DatabaseConnectionGenerator... values)
      throws AlgorithmConfigurationException{
    builder.databaseConnectionGenerator(values[0]);
    this.databaseConnectionGenerator = values[0];

    System.out.println("##############################################################");
    System.out.println("database generator : "+ identifier +"; values :"+ values[0]);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.build();
    System.out.println("database: "+ this.databaseConnectionGenerator);
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