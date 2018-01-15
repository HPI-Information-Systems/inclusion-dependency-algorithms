package de.metanome.validation.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.io.IOException;
import java.io.InputStream;
import org.jooq.DSLContext;
import org.jooq.Loader;
import org.jooq.LoaderFieldMapper.LoaderFieldContext;
import org.jooq.Record;

class Helper {

  static void loadCsv(final DSLContext context, final String fileName, final String tableName)
      throws IOException {

    try (InputStream in = Helper.class.getResourceAsStream(fileName)) {

      final Loader<Record> result = context.loadInto(table(name(tableName)))
          .loadCSV(in)
          .fields(LoaderFieldContext::field)
          .execute();

      assertThat(result.errors()).isEmpty();
    }
  }
}
