package de.metanome.validation.database;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.ExecuteContext;
import org.jooq.Field;
import org.jooq.Loader;
import org.jooq.LoaderError;
import org.jooq.LoaderFieldMapper.LoaderFieldContext;
import org.jooq.Record;
import org.jooq.impl.DefaultExecuteListener;
import org.jooq.impl.DefaultExecuteListenerProvider;

class Helper {

  private static final DSLContextFactory contextFactory = new DSLContextFactory();

  static DSLContext createInMemoryContext() throws SQLException {
    final Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:myDb");
    return contextFactory.create(connection);
  }

  static DSLContext createInMemoryDebugContext() throws SQLException {
    final DSLContext context = createInMemoryContext();
    setUpIntrospection(context);
    return context;
  }

  private static void setUpIntrospection(final DSLContext context) {
    context.configuration().set(new DefaultExecuteListenerProvider(new DebugExecuteListener()));
  }

  private static class DebugExecuteListener extends DefaultExecuteListener {

    @Override
    public void start(final ExecuteContext ctx) {
      System.err.println("Query: " + ctx.query().getSQL());
    }
  }

  static void loadCsv(final DSLContext context, final String fileName, final String tableName)
      throws IOException {

    try (InputStream in = Helper.class.getResourceAsStream(fileName)) {

      final Loader<Record> result = context.loadInto(table(name(tableName)))
          .loadCSV(in)
          .fields(LoaderFieldContext::field)
          .execute();

      assertThat(result.errors()).as(getMessage(result.errors())).isEmpty();
    }
  }

  /**
   * Load a CSV file with {@code NULL} values which are represented as star {@code *}.
   *
   * <p>This method cannot possibly be merged with the other one since the {@code nullString(...)}
   * API is only available when explicitly passing a collection of o fields to {@code loadCsv},
   * which is actually quite redundant.</p>
   */
  static void loadCsvWithNulls(final DSLContext context, final String fileName,
      final String tableName, final List<String> fieldNames) throws IOException {

    try (InputStream in = Helper.class.getResourceAsStream(fileName)) {

      final Loader<Record> result = context.loadInto(table(name(tableName)))
          .loadCSV(in)
          .fields(toFields(tableName, fieldNames))
          .nullString("*")
          .execute();

      assertThat(result.errors()).as(getMessage(result.errors())).isEmpty();
    }
  }

  private static String getMessage(final List<LoaderError> errors) {
    return errors.stream().map(e -> e.exception().getMessage()).collect(joining(", "));
  }

  private static Collection<? extends Field<?>> toFields(final String tableName,
      final Collection<String> names) {

    return names.stream().map(name -> field(name(tableName, name))).collect(toList());
  }
}
