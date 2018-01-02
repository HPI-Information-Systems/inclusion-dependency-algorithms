package de.metanome.input.ind;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementListBox;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.util.List;

public class InclusionDependencyInputConfigurationRequirements {

  private static final String ALGORITHM = "ind-input-algorithm";
  private static final String CONFIGURATION = "ind-input-configuration";

  public static List<ConfigurationRequirement<?>> indInput() {
    final List<String> names = getAvailableAlgorithmNames();

    final ConfigurationRequirementListBox algorithms = new ConfigurationRequirementListBox(
        ALGORITHM, names, 1, 1);
    algorithms.setDefaultValues(new String[]{names.get(0)});

    final ConfigurationRequirementString configuration = new ConfigurationRequirementString(
        CONFIGURATION, 1);
    configuration.setRequired(false);

    return asList(algorithms, configuration);
  }

  public static List<String> getAvailableAlgorithmNames() {
    return getAvailableAlgorithms().stream().map(AlgorithmType::name).collect(toList());
  }

  public static List<AlgorithmType> getAvailableAlgorithms() {
    return asList(AlgorithmType.values());
  }

  public static boolean acceptListBox(final String identifier, final String[] selectedValues,
      final InclusionDependencyParameters parameters) {

    if (identifier.equals(ALGORITHM)) {
      final AlgorithmType algorithm = AlgorithmType.valueOf(selectedValues[0]);
      parameters.setAlgorithmType(algorithm);
      return true;
    }

    return false;
  }

  public static boolean acceptString(final String identifier, final String[] values,
      final InclusionDependencyParameters parameters) {

    if (identifier.equals(CONFIGURATION)) {
      parameters.setConfigurationString(values[0]);
      return true;
    }

    return false;
  }

  public static void acceptTableInputGenerator(final TableInputGenerator[] values,
      final InclusionDependencyParameters parameters) {

    parameters.setTableInputGenerators(asList(values));
  }

  public static void acceptRelationalInputGenerators(final RelationalInputGenerator[] values,
      final InclusionDependencyParameters parameters) {

    parameters.setRelationalInputGenerators(asList(values));
  }
}
