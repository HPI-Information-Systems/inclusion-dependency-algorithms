package de.metanome.validation;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.validation.database.DatabaseValidationFactory;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ValidationStrategyFactory {

  private final DatabaseValidationFactory databaseValidationFactory;

  public ValidationStrategyFactory() {
    databaseValidationFactory = new DatabaseValidationFactory();
  }

  public ValidationStrategy forDatabase(final ValidationParameters parameters) {
    final DatabaseConnectionGenerator generator = parameters.getConnectionGenerator();
    ensureConnected(generator);
    return databaseValidationFactory.create(generator.getConnection(), parameters.getQueryType());
  }

  // https://github.com/HPI-Information-Systems/Metanome/issues/385
  private void ensureConnected(final DatabaseConnectionGenerator generator) {
    final String nopSql = "select 1 union select 1";
    try (final ResultSet ignored = generator.generateResultSetFromSql(nopSql)) {
      // nop
    } catch (final AlgorithmExecutionException | SQLException e) {
      e.printStackTrace();
    }
  }
}
