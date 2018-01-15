package de.metanome.validation.database;

import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.SelectField;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

class JooqAdapter {

  static Collection<? extends SelectField<?>> fields(final ColumnPermutation columns) {
    final List<SelectField<Object>> fields = new ArrayList<>();
    for (final ColumnIdentifier column : columns.getColumnIdentifiers()) {
      fields.add(toField(column));
    }
    return fields;
  }

  static Collection<? extends TableLike<?>> tables(final ColumnPermutation... columns) {
    return Arrays.stream(columns)
        .flatMap(columnPermutation -> columnPermutation.getColumnIdentifiers().stream())
        .map(ColumnIdentifier::getTableIdentifier)
        .distinct()
        .map(DSL::name).map(DSL::table)
        .collect(toList());
  }

  static Condition isNull(final ColumnPermutation columns) {
    return columns.getColumnIdentifiers().stream()
        .map(column -> toField(column).isNull())
        .reduce(Condition::and)
        .orElseThrow(IllegalArgumentException::new);
  }

  static Condition notNull(final ColumnPermutation columns) {
    return columns.getColumnIdentifiers().stream()
        .map(column -> toField(column).isNotNull())
        .reduce(Condition::and)
        .orElseThrow(IllegalArgumentException::new);
  }

  static Condition oneNotNull(final ColumnPermutation columns) {
    return columns.getColumnIdentifiers().stream()
        .map(column -> toField(column).isNotNull())
        .reduce(Condition::or)
        .orElseThrow(IllegalArgumentException::new);
  }

  static Field<Object> toField(final ColumnIdentifier column) {
    return field(name(column.getTableIdentifier(), column.getColumnIdentifier()));
  }

  static Condition[] columnsEqual(final ColumnPermutation lhs, final ColumnPermutation rhs) {
    final Condition[] conditions = new Condition[lhs.getColumnIdentifiers().size()];
    for (int index = 0; index < lhs.getColumnIdentifiers().size(); ++index) {
      conditions[index] = toField(lhs.getColumnIdentifiers().get(index))
          .eq(toField(rhs.getColumnIdentifiers().get(index)));
    }
    return conditions;
  }
}
