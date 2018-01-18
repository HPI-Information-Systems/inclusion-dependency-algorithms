package de.metanome.validation.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.name;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.validation.ErrorMarginValidationResult;
import de.metanome.validation.ValidationResult;
import java.io.IOException;
import org.jooq.DSLContext;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryAcceptanceTest {

  private DSLContext context;

  private final Queries queries = new Queries();

  @BeforeEach
  void setUp() throws Exception {
    context = Helper.createInMemoryContext();
    createPersonRelation();
  }

  @AfterEach
  void tearDown() {
    context.dropTableIfExists(name("person")).execute();
  }

  @Test
  void errorMarginQuery_containsCorrelatedSubquery() throws Exception {
    final InclusionDependency toRefute = InclusionDependencyBuilder
        .dependent().column("person", "name")
        .referenced().column("person", "second_name")
        .build();

    final DatabaseValidation validation = new DatabaseValidation(context,
        queries.get(QueryType.ERROR_MARGIN));

    final ErrorMarginValidationResult result = (ErrorMarginValidationResult) validation
        .validate(toRefute);
    assertThat(result.getErrorMargin()).isEqualTo(0.25);
    assertThat(result.isValid()).isFalse();
  }

  @Test
  void notExistsQuery_containsCorrelatedSubquery() throws Exception {
    final InclusionDependency toRefute = InclusionDependencyBuilder
        .dependent().column("person", "name")
        .referenced().column("person", "second_name")
        .build();

    final DatabaseValidation validation = new DatabaseValidation(context,
        queries.get(QueryType.NOT_EXISTS));

    final ValidationResult result = validation.validate(toRefute);

    assertThat(result.isValid()).isFalse();
  }

  private void createPersonRelation() throws IOException {
    context.createTable(name("person"))
        .column("name", SQLDataType.VARCHAR(5))
        .column("second_name", SQLDataType.VARCHAR(5))
        .execute();

    Helper.loadCsv(context, "person.csv", "person");
  }

}
