package de.metanome.algorithms.zigzag;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithms.zigzag.configuration.ConfigurationKey;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration.ZigzagConfigurationBuilder;
import de.metanome.input.ind.InclusionDependencyInputConfigurationRequirements;
import de.metanome.input.ind.InclusionDependencyInputParameterAlgorithm;
import de.metanome.input.ind.InclusionDependencyParameters;
import de.metanome.validation.InclusionDependencyValidationAlgorithm;
import de.metanome.validation.ValidationConfigurationRequirements;
import de.metanome.validation.ValidationParameters;
import java.util.ArrayList;
import java.util.List;

public abstract class ZigzagAlgorithm implements InclusionDependencyAlgorithm,
    IntegerParameterAlgorithm,
    InclusionDependencyValidationAlgorithm,
    InclusionDependencyInputParameterAlgorithm {

  private final ZigzagConfigurationBuilder configurationBuilder;
  final ValidationParameters validationParameters;
  final InclusionDependencyParameters indInputParams;

  ZigzagAlgorithm() {
    configurationBuilder = ZigzagConfiguration.builder();
    validationParameters = new ValidationParameters();
    indInputParams = new InclusionDependencyParameters();
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    configurationBuilder.validationParameters(validationParameters)
        .inclusionDependencyParameters(indInputParams);
    Zigzag zigzag = new Zigzag(configurationBuilder.build());
    zigzag.execute();
  }

  List<ConfigurationRequirement<?>> common() {
    final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    ConfigurationRequirementInteger startKConfig = new ConfigurationRequirementInteger(
        ConfigurationKey.START_K.name());
    startKConfig.setDefaultValues(new Integer[]{2});
    requirements.add(startKConfig);

    ConfigurationRequirementInteger epsilonConfig = new ConfigurationRequirementInteger(
        ConfigurationKey.EPSILON.name());
    startKConfig.setDefaultValues(new Integer[]{10000});
    requirements.add(epsilonConfig);

    requirements.addAll(ValidationConfigurationRequirements.validationStrategy());
    requirements.addAll(InclusionDependencyInputConfigurationRequirements.indInput());
    return requirements;
  }

  @Override
  public void setIntegerConfigurationValue(String identifier, Integer... values) {
    if (identifier.equals(ConfigurationKey.START_K.name())) {
      configurationBuilder.startK(values[0]);
    } else if (identifier.equals(ConfigurationKey.EPSILON.name())) {
      configurationBuilder.epsilon(values[0]);
    }
  }

  @Override
  public void setListBoxConfigurationValue(String identifier, String... selectedValues) {
    InclusionDependencyInputConfigurationRequirements.acceptListBox(identifier, selectedValues,
        indInputParams);
    ValidationConfigurationRequirements
        .acceptListBox(identifier, selectedValues, validationParameters);
  }

  @Override
  public void setStringConfigurationValue(String identifier, String... values) {
    InclusionDependencyInputConfigurationRequirements.acceptString(identifier, values,
        indInputParams);
  }

  @Override
  public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
    configurationBuilder.resultReceiver(resultReceiver);
  }

  @Override
  public String getAuthors() {
    return "Nils Strelow, Fabian Windheuser, Axel Stebner";
  }

  @Override
  public String getDescription() {
    return
        "Implementation of 'Zigzag: A new algorithm for discovering large inclusion dependencies in "
            +
            "relational databases' by De Marchi, Petit, 2003";
  }
}