package de.metanome.validation.database;

import de.metanome.validation.ValidationStrategy;
import java.sql.Connection;
import org.jooq.DSLContext;

public class DatabaseValidationFactory {

  private final Queries queries;
  private final DSLContextFactory contextFactory;

  public DatabaseValidationFactory() {
    queries = new Queries();
    contextFactory = new DSLContextFactory();
  }

  public ValidationStrategy create(final Connection connection, final QueryType queryType) {
    final Query query = queries.get(queryType);
    final DSLContext context = contextFactory.create(connection);
    return new DatabaseValidation(context, query);
  }
}
