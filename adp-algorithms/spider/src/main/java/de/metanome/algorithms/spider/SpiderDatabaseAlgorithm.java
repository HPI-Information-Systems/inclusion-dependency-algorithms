package de.metanome.algorithms.spider;

import static de.metanome.algorithms.spider.ConfigurationKey.TABLE;
import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.TableInputParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementTableInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.util.ArrayList;
import java.util.List;

public class SpiderDatabaseAlgorithm extends SpiderAlgorithm implements
    TableInputParameterAlgorithm {

  @Override
  public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
    final ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.addAll(database());
    requirements.addAll(common());
    return requirements;
  }

  private List<ConfigurationRequirement<?>> database() {
    final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(new ConfigurationRequirementTableInput(TABLE.name(),
        ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
    return requirements;
  }

  @Override
  public void setTableInputConfigurationValue(String identifier, TableInputGenerator... values)
      throws AlgorithmConfigurationException {

    if (identifier.equals(TABLE.name())) {
      builder.tableInputGenerators(asList(values));
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }
}
