package de.metanome.algorithms.spider;

import static de.metanome.algorithms.spider.ConfigurationKey.TABLE;
import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import java.util.ArrayList;
import java.util.List;

public class SpiderFileAlgorithm extends SpiderAlgorithm implements
    RelationalInputParameterAlgorithm {

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.addAll(file());
    requirements.addAll(common());
    return requirements;
  }

  private List<ConfigurationRequirement<?>> file() {
    return asList(new ConfigurationRequirementRelationalInput(TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
  }

  @Override
  public void setRelationalInputConfigurationValue(String identifier,
      RelationalInputGenerator... values)
      throws AlgorithmConfigurationException {

    if (identifier.equals(TABLE.name())) {
      builder.relationalInputGenerators(asList(values));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

}
