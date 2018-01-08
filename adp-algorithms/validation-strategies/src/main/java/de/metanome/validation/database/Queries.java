package de.metanome.validation.database;

import static de.metanome.validation.database.JooqAdapter.columnsEqual;
import static de.metanome.validation.database.JooqAdapter.fields;
import static de.metanome.validation.database.JooqAdapter.isNull;
import static de.metanome.validation.database.JooqAdapter.notNull;
import static de.metanome.validation.database.JooqAdapter.oneNotNull;
import static de.metanome.validation.database.JooqAdapter.tables;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectFrom;
import static org.jooq.impl.DSL.table;

import com.google.common.collect.ImmutableMap;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.validation.DefaultValidationResult;
import de.metanome.validation.ValidationResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;


class Queries {

  private final Map<QueryType, Query> queries;

  Queries() {
    queries = ImmutableMap.of(QueryType.NOT_IN, this::notIn,
        QueryType.NOT_EXISTS, this::notExists,
        QueryType.LEFT_OUTER_JOIN, this::leftOuterJoin,
        QueryType.EXCEPT, this::except);
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
        ).limit(1)
    ).fetchOne().value1();

    return new DefaultValidationResult(violators == 0);
  }

  private ValidationResult leftOuterJoin(final DSLContext context, final ColumnPermutation lhs,
      final ColumnPermutation rhs) {

    final String rhsTableName = rhs.getColumnIdentifiers().get(0).getTableIdentifier();
    final Table rhsAlias = table(name(rhsTableName)).asTable(randomAlias());
    final ColumnPermutation rhsAliasColumns = swapRelationName(rhsAlias.getName(), rhs);

    final int violators = context.selectDistinct(fields(lhs))
        .from(tables(lhs))
        .leftOuterJoin(rhsAlias)
        .on(columnsEqual(lhs, rhsAliasColumns))
        .where(isNull(rhsAliasColumns))
        .and(oneNotNull(lhs))
        .limit(1)
        .execute();

    return new DefaultValidationResult(violators == 0);
  }

  private ValidationResult except(final DSLContext context, final ColumnPermutation lhs,
      final ColumnPermutation rhs) {

    final int violators =
        context.selectCount().from(
            select(fields(lhs)).from(tables(lhs)).where(notNull(lhs))
                .except(select(fields(rhs)).from(tables(rhs))).limit(1))
            .fetchOne().value1();

    return new DefaultValidationResult(violators == 0);
  }

  private ColumnPermutation swapRelationName(final String relation,
      final ColumnPermutation columns) {
    final List<ColumnIdentifier> result = new ArrayList<>();
    for (final ColumnIdentifier column : columns.getColumnIdentifiers()) {
      result.add(new ColumnIdentifier(relation, column.getColumnIdentifier()));
    }

    final ColumnPermutation columnPermutation = new ColumnPermutation();
    columnPermutation.setColumnIdentifiers(result);
    return columnPermutation;
  }

  private String randomAlias() {
    // cheap copy of org.jooq.impl.SelectQueryImpl.asTable()
    return "alias_" + System.currentTimeMillis();
  }
}
