package de.metanome.algorithms.demarchi;

import static java.util.Arrays.asList;

import com.google.common.base.Joiner;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.ArrayList;

public class DeMarchiAlgorithm implements InclusionDependencyAlgorithm,
    TableInputParameterAlgorithm,
    RelationalInputParameterAlgorithm {

  private final DeMarchi impl;
  private final Configuration.ConfigurationBuilder builder;

  public DeMarchiAlgorithm() {
    impl = new DeMarchi();
    builder = Configuration.builder();
  }

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(tableInput());
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
      final TableInputGenerator... values) throws AlgorithmConfigurationException {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.tableInputGenerators(asList(values));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

  @Override
  public void setRelationalInputConfigurationValue(final String identifier,
      final RelationalInputGenerator... values) throws AlgorithmConfigurationException {

    if (identifier.equals(ConfigurationKey.TABLE.name())) {
      builder.relationalInputGenerators(asList(values));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

  @SafeVarargs
  private final <T> void handleUnknownConfiguration(final String identifier, final T... values)
      throws AlgorithmConfigurationException {

    final String formattedValues = Joiner.on(", ").join(values);
    final String message = String
        .format("unknown configuration '%s', values: '%s'", identifier, formattedValues);
    throw new AlgorithmConfigurationException(message);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    final Configuration configuration = builder.build();
    impl.execute(configuration);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch, Tim Friedrich";
  }

  @Override
  public String getDescription() {
    return "DeMarchi";
  }
}