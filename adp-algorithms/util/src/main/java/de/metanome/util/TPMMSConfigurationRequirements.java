package de.metanome.util;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import java.util.List;

public class TPMMSConfigurationRequirements {

  private static final String INPUT_ROW_LIMIT = "INPUT_ROW_LIMIT";
  private static final String MAX_MEMORY_USAGE_PERCENTAGE = "MAX_MEMORY_USAGE_PERCENTAGE";
  private static final String MEMORY_CHECK_INTERVAL = "MEMORY_CHECK_INTERVAL";

  public static List<ConfigurationRequirement<?>> tpmms() {
    final TPMMSConfiguration defaultValues = TPMMSConfiguration.withDefaults();

    final ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(
        INPUT_ROW_LIMIT);
    inputRowLimit.setDefaultValues(new Integer[]{defaultValues.getInputRowLimit()});

    final ConfigurationRequirementInteger maxMemoryUsage = new ConfigurationRequirementInteger(
        MAX_MEMORY_USAGE_PERCENTAGE);
    maxMemoryUsage.setDefaultValues(new Integer[]{defaultValues.getMaxMemoryUsagePercentage()});

    final ConfigurationRequirementInteger memoryCheckInterval = new ConfigurationRequirementInteger(
        MEMORY_CHECK_INTERVAL);
    memoryCheckInterval.setDefaultValues(new Integer[]{defaultValues.getMemoryCheckInterval()});

    return asList(inputRowLimit, maxMemoryUsage, memoryCheckInterval);
  }

  public static boolean acceptInteger(final String identifier, final Integer[] values,
      final TPMMSConfiguration configuration) {

    final int value = values[0];
    if (identifier.equals(INPUT_ROW_LIMIT)) {
      configuration.setInputRowLimit(value);
      return true;
    }

    if (identifier.equals(MAX_MEMORY_USAGE_PERCENTAGE)) {
      configuration.setMaxMemoryUsagePercentage(value);
      return true;
    }

    if (identifier.equals(MEMORY_CHECK_INTERVAL)) {
      configuration.setMemoryCheckInterval(value);
      return true;
    }

    return false;
  }

}
