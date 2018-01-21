package de.metanome.validation.database;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.validation.ValidationResult;
import org.jooq.DSLContext;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class EmptyColumnHandlingTest {

  private static final String TABLE = "tableWithEmptyColumns";

  private DSLContext context;

  private final Queries queries = new Queries();

  @BeforeEach
  void setUp() throws Exception {
    context = Helper.createInMemoryContext();

    context.createTable(TABLE)
        .column("a", SQLDataType.INTEGER)
        .column("b", SQLDataType.INTEGER)
        .execute();

    Helper.loadCsvWithNulls(context, "emptyColumn.csv", TABLE, asList("a", "b"));
  }

  @AfterEach
  void tearDown() {
    context.dropTableIfExists(TABLE).execute();
    context.close();
  }

  @ParameterizedTest
  @EnumSource(QueryType.class)
  void emptyLhs(final QueryType queryType) {
    final DatabaseValidation validation = new DatabaseValidation(context, queries.get(queryType));

    final ValidationResult result = validation.validate(emptyAndNonEmpty());

    assertThat(result.isValid()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(QueryType.class)
  void emptyRhs(final QueryType queryType) {
    final DatabaseValidation validation = new DatabaseValidation(context, queries.get(queryType));

    final ValidationResult result = validation.validate(nonEmptyAndEmpty());

    assertThat(result.isValid()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(QueryType.class)
  void emptyIsSubsetOfEmpty(final QueryType queryType) {
    final DatabaseValidation validation = new DatabaseValidation(context, queries.get(queryType));

    final ValidationResult result = validation.validate(emptyAndEmpty());

    assertThat(result.isValid()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(QueryType.class)
  void trivialIndSatisfied(final QueryType queryType) {
    final DatabaseValidation validation = new DatabaseValidation(context, queries.get(queryType));

    final ValidationResult result = validation.validate(nonEmptyAndNonEmpty());

    assertThat(result.isValid()).isTrue();
  }

  private InclusionDependency emptyAndNonEmpty() {
    return ind("a", "b");
  }

  private InclusionDependency nonEmptyAndEmpty() {
    return ind("b", "a");
  }

  private InclusionDependency emptyAndEmpty() {
    return ind("a", "a");
  }

  private InclusionDependency nonEmptyAndNonEmpty() {
    return ind("b", "b");
  }

  private InclusionDependency ind(final String dep, final String ref) {
    return InclusionDependencyBuilder.dependent().column(TABLE, dep)
        .referenced().column(TABLE, ref).build();
  }
}
