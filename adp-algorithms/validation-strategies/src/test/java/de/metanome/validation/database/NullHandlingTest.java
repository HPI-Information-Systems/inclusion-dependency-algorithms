package de.metanome.validation.database;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.field;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.validation.ValidationResult;
import org.jooq.DSLContext;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NullHandlingTest {

  private DSLContext context;

  private final Queries queries = new Queries();

  @BeforeEach
  void setUp() throws Exception {
    context = Helper.createInMemoryContext();

    context.createTable("tableX")
        .column("K", SQLDataType.CHAR(1))
        .column("L", SQLDataType.CHAR(1))
        .column("M", SQLDataType.CHAR(1))
        .column("N", SQLDataType.CHAR(1))
        .execute();

    Helper.loadCsvWithNulls(context, "tableX.csv", "tableX", asList("K", "L", "M", "N"));
  }

  @AfterEach
  void tearDown() {
    context.dropTableIfExists("tableX").execute();
    context.close();
  }

  @Test
  void checkNullValuesPresent() {
    final int nulls = context.selectCount().from("tableX").where(field("K").isNull())
        .fetchOne().component1();

    assertThat(nulls)
        .as("NULL values must be present in the DB else this test is pointless")
        .isNotZero();
  }

  @ParameterizedTest
  @EnumSource(QueryType.class)
  void allQueriesShouldHandleNullValueForUnary(final QueryType queryType) {
    final DatabaseValidation validation = new DatabaseValidation(context, queries.get(queryType));

    final ValidationResult result = validation.validate(getValidUnaryWithNulls());

    assertThat(result.isValid()).isTrue();
  }

  private InclusionDependency getValidUnaryWithNulls() {
    return InclusionDependencyBuilder
        .dependent().column("tableX", "K")
        .referenced().column("tableX", "L").build();
  }

  @ParameterizedTest
  @EnumSource(QueryType.class)
  void allQueriesShouldSkipTupleWithNullGivenNary(final QueryType queryType) {
    final DatabaseValidation validation = new DatabaseValidation(context, queries.get(queryType));

    final ValidationResult result = validation.validate(getValidNaryWithNulls());

    assertThat(result.isValid()).isTrue();
  }

  private InclusionDependency getValidNaryWithNulls() {
    return InclusionDependencyBuilder
        .dependent()
        .column("tableX", "K")
        .column("tableX", "L")

        .referenced()
        .column("tableX", "M")
        .column("tableX", "N")
        .build();
  }

}
