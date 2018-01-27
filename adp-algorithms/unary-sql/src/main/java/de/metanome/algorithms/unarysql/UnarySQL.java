package de.metanome.algorithms.unarysql;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;


public class UnarySQL {

  private final TableInfoFactory tableInfoFactory;
  private final ValidationStrategyFactory validationStrategyFactory;

  private Configuration configuration;
  private List<TableInfo> tables;
  private ValidationStrategy validationStrategy;


  public UnarySQL() {
    tableInfoFactory = new TableInfoFactory();
    validationStrategyFactory = new ValidationStrategyFactory();
  }

  public void execute(final Configuration configuration)
      throws AlgorithmExecutionException {

    initialize(configuration);

    final List<InclusionDependency> candidates = getCandidates();
    for (final InclusionDependency candidate : candidates) {
      if (isInd(candidate)) {
        configuration.getResultReceiver().receiveResult(candidate);
      }
    }

    validationStrategy.close();
  }

  private void initialize(final Configuration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;

    tables = tableInfoFactory
        .createFromTableInputs(configuration.getTableInputGenerators());

    validationStrategy = validationStrategyFactory
        .forDatabase(configuration.getValidationParameters());
  }

  private List<InclusionDependency> getCandidates() {
    try (DSLContext context = getContext()) {
      return genLevel1Candidates(context);
    }
  }

  private DSLContext getContext() {
    // Tables can only be taken from the very same source database
    final TableInputGenerator firstInput = configuration.getTableInputGenerators().get(0);
    return DSL.using(firstInput.getDatabaseConnectionGenerator().getConnection());
  }

  private List<InclusionDependency> genLevel1Candidates(final DSLContext context) {
    final List<ColumnIdentifier> attributes = new ArrayList<>();
    final List<InclusionDependency> candidates = new ArrayList<>();

    for (final TableInfo table : tables) {
      for (String column : table.getColumnNames()) {
        if (configuration.isProcessEmptyColumns() || isNonEmpty(context, table, column)) {
          attributes.add(new ColumnIdentifier(table.getTableName(), column));
        }
      }
    }

    for (final ColumnIdentifier lhs : attributes) {
      for (final ColumnIdentifier rhs : attributes) {
        if (!lhs.equals(rhs)) {
          final InclusionDependency candidate = new InclusionDependency(new ColumnPermutation(lhs),
              new ColumnPermutation(rhs));
          candidates.add(candidate);
        }
      }
    }

    return candidates;
  }

  private boolean isInd(final InclusionDependency ind) {
    return validationStrategy.validate(ind).isValid();
  }

  private boolean isNonEmpty(final DSLContext context, final TableInfo table, final String column) {
    return context.fetchCount(
        context.selectDistinct(DSL.field(DSL.name(column))).from(DSL.name(table.getTableName()))
    ) > 1; /* NULL is one distinct value */
  }
}
