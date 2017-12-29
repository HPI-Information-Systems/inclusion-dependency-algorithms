package de.metanome.util;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

@Getter
@Builder
public class TestDatabase {

  private final Class<?> resourceClass;
  private final String relationName;
  private final List<String> columnNames;
  private final String csvPath;

  private DSLContext context;
  private Connection connection;


  public void setUp() throws Exception {
    connection = DriverManager.getConnection("jdbc:hsqldb:mem:myDb");
    context = DSL.using(connection, SQLDialect.HSQLDB, new Settings().withRenderNameStyle(
        RenderNameStyle.AS_IS));

    final List<Field<String>> fields = getFields();

    context.createTable(relationName)
        .columns(fields)
        .execute();

    try (InputStream in = resourceClass.getResourceAsStream(csvPath)) {
      context.loadInto(table(name(relationName)))
          .loadCSV(in)
          .fields(fields)
          .execute();
    }
  }

  public void tearDown() {
    if (context != null) {
      context.close();
    }
  }

  private List<Field<String>> getFields() {
    final List<Field<String>> fields = new ArrayList<>();
    for (int index = 0; index < columnNames.size(); ++index) {
      fields.add(field(name(columnNames.get(index)), SQLDataType.VARCHAR(10)));
    }
    return fields;
  }

  public DatabaseConnectionGenerator asConnectionGenerator() {
    final DatabaseConnectionGenerator generator = mock(DatabaseConnectionGenerator.class);
    given(generator.getConnection()).willReturn(connection);
    return generator;
  }

  public TableInputGenerator asTableInputGenerator() {
    final TableInputGenerator generator = mock(TableInputGenerator.class);

    try {
      willAnswer(invocation ->
          connection.prepareStatement(String.format("select * from %s;", relationName))
              .executeQuery()
      ).given(generator).select();
    } catch (final AlgorithmExecutionException ignored) {
    }

    return generator;
  }

}
