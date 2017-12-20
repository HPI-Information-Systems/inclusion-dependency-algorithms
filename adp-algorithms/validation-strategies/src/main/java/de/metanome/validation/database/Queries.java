package de.metanome.validation.database;

import static de.metanome.validation.database.JooqAdapter.fields;
import static de.metanome.validation.database.JooqAdapter.notNull;
import static de.metanome.validation.database.JooqAdapter.tables;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectFrom;

import com.google.common.collect.ImmutableMap;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.validation.DefaultValidationResult;
import de.metanome.validation.ValidationResult;
import java.util.Map;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;


class Queries {

  private final Map<QueryType, Query> queries;

  Queries() {
    queries = ImmutableMap.of(QueryType.NOT_IN, this::notIn,
        QueryType.NOT_EXISTS, this::notExists);
  }

  Query get(final QueryType type) {
    return queries.get(type);
  }

  private ValidationResult notIn(final DSLContext context, final ColumnPermutation lhs,
      final ColumnPermutation rhs) {

    final int violators = context.selectCount().from(
        context.select(fields(lhs))
            .from(tables(lhs, rhs))
            .where(notNull(lhs))
            .and(row(fields(lhs)).notIn(select(fields(rhs)).from(tables(rhs)).where(notNull(rhs))))
            .asTable("indCheck"))
        .fetchOne().value1();

    return new DefaultValidationResult(violators == 0);
  }

  private ValidationResult notExists(final DSLContext context, final ColumnPermutation lhs,
      final ColumnPermutation rhs) {

    final Table<Record> lhsAlias = context.select(fields(lhs))
        .from(tables(lhs))
        .where(notNull(lhs))
        .asTable();

    final int violators = context.selectCount().from(
        selectFrom(lhsAlias).whereNotExists(
            context.selectOne().from(tables(rhs)).where(row(fields(rhs)).eq(row(lhsAlias.fields())))
        )
    ).fetchOne().value1();

    return new DefaultValidationResult(violators == 0);
  }
}
