package de.metanome.validation.database;

import java.sql.Connection;
import java.sql.SQLException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;

class DSLContextFactory {

  DSLContext create(final Connection connection) {
    try {
      final SQLDialect dialect = JDBCUtils.dialect(connection.getMetaData().getURL());

      if (dialect == SQLDialect.HSQLDB) {
        return hsqldb(connection);
      }

      return defaultContext(connection);
    } catch (final SQLException e) {
      return defaultContext(connection);
    }
  }

  private DSLContext defaultContext(final Connection connection) {
    return DSL.using(connection);
  }

  private DSLContext hsqldb(final Connection connection) {
    final Settings settings = new Settings().withRenderNameStyle(RenderNameStyle.AS_IS);
    return DSL.using(connection, SQLDialect.HSQLDB, settings);
  }
}
