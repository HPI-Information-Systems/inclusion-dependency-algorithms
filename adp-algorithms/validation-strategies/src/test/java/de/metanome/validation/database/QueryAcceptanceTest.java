package de.metanome.validation.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.validation.ValidationResult;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import org.jooq.DSLContext;
import org.jooq.LoaderFieldMapper.LoaderFieldContext;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryAcceptanceTest {

  private DSLContext context;

  private final DSLContextFactory contextFactory = new DSLContextFactory();
  private final Queries queries = new Queries();

  @BeforeEach
  void setUp() throws Exception {
    final Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:myDb");
    context = contextFactory.create(connection);
  }

  @Test
  void notExistsQuery_containsCorrelatedSubquery() throws Exception {
    createPersonRelation();
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
