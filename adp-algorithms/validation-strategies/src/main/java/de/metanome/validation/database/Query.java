package de.metanome.validation.database;

import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.validation.ValidationResult;
import org.jooq.DSLContext;

@FunctionalInterface
interface Query {

  default ValidationResult check(final DSLContext context, final InclusionDependency ind) {
    return check(context, ind.getDependant(), ind.getReferenced());
  }

  ValidationResult check(DSLContext context, ColumnPermutation lhs, ColumnPermutation rhs);
}
