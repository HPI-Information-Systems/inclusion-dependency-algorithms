package de.metanome.validation.database;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.validation.ValidationResult;
import de.metanome.validation.ValidationStrategy;
import org.jooq.DSLContext;

class DatabaseValidation implements ValidationStrategy {

  private final DSLContext context;
  private final Query query;

  DatabaseValidation(final DSLContext context, final Query query) {
    this.context = context;
    this.query = query;
  }

  @Override
  public ValidationResult validate(final InclusionDependency toCheck) {
    return query.check(context, toCheck);
  }

  @Override
  public void close() {
    context.close();
  }
}
