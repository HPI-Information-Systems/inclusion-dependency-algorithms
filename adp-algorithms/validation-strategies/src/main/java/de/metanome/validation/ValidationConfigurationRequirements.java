package de.metanome.validation;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementListBox;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.validation.database.QueryType;
import java.util.List;

public class ValidationConfigurationRequirements {

  private static final String QUERY_KEY = "validation-query";

  public static List<ConfigurationRequirement<?>> validationStrategy() {
    final List<String> names = availableQueryNames();

    final ConfigurationRequirementListBox queryTypes = new ConfigurationRequirementListBox(
        QUERY_KEY, names, 1, 1);
    queryTypes.setDefaultValues(new String[]{names.get(0)});

    return asList(queryTypes);
  }

  public static List<QueryType> availableQueries() {
    return asList(QueryType.values());
  }

  public static List<String> availableQueryNames() {
    return availableQueries().stream().map(QueryType::name).collect(toList());
  }

  public static boolean acceptListBox(final String identifier, final String[] selectedValues,
      final ValidationParameters parameters) {

    if (identifier.equals(QUERY_KEY)) {
      final QueryType queryType = QueryType.valueOf(selectedValues[0]);
      parameters.setQueryType(queryType);
      return true;
    }

    return false;
  }

  public static void acceptDatabaseConnectionGenerator(final DatabaseConnectionGenerator[] values,
      final ValidationParameters parameters) {

    parameters.setConnectionGenerator(values[0]);
  }
}
