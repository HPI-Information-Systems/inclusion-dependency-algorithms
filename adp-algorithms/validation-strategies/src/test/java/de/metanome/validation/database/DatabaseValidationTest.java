package de.metanome.validation.database;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.quicktheories.generators.Generate.enumValues;
import static org.quicktheories.generators.Generate.pick;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.validation.ValidationResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.Data;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quicktheories.WithQuickTheories;

class DatabaseValidationTest implements WithQuickTheories {

  private DSLContext context;

  private final DSLContextFactory contextFactory = new DSLContextFactory();
  private final Queries queries = new Queries();

  /**
   * A B  C D  E
   * 1 2  3 2  NULL
   * 2 1  1 2  NULL
   * 3 2  2 1
   * 4 4  3 3
   * ---- 4 4
   */
  @BeforeEach
  void setUp() throws Exception {
    final Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:myDb");
    context = contextFactory.create(connection);

    context.insertInto(createTableAndColumn("A")).values(1, 2, 3, 4).execute();
    context.insertInto(createTableAndColumn("B")).values(2, 1, 2, 4).execute();
    context.insertInto(createTableAndColumn("C")).values(3, 2, 1, 3, 4).execute();
    context.insertInto(createTableAndColumn("D")).values(2, 2, 1, 3, 4).execute();

    context.insertInto(createTableAndColumn("E")).values(null, null).execute();
  }

  private Table<?> createTableAndColumn(final String name) {
    final Table<?> table = table(name("table" + name));
    context.createTable(table).column(name, SQLDataType.INTEGER).execute();
    return table;
  }

  @AfterEach
  void tearDown() {
    context.close();
  }

  @Test
  void allQueriesShouldDetectValidAndInvalidCandidates() {
    qt().forAll(enumValues(QueryType.class), pick(allCandidates())).checkAssert(this::isValid);
  }

  private void isValid(final QueryType queryType, final Candidate candidate) {
    final Query query = queries.get(queryType);
    final DatabaseValidation validation = new DatabaseValidation(context, query);

    final ValidationResult result = validation.validate(candidate.getInd());

    assertThat(result.isValid())
        .as("candidate %s, queryType %s", candidate, queryType)
        .isEqualTo(candidate.isValid());
  }

  private List<Candidate> allCandidates() {
    final List<Candidate> candidates = new ArrayList<>();
    candidates.addAll(valid());
    candidates.addAll(invalid());
    return candidates;
  }

  private List<Candidate> valid() {
    return Stream.of(bSubsetOfA(), abSubsetOfCD(), eSubsetOfA())
        .map(Candidate::valid).collect(toList());
  }

  private List<Candidate> invalid() {
    return Stream.of(aSubsetOfB(), cdSubsetOfAB(), aSubsetOfE())
        .map(Candidate::invalid).collect(toList());
  }

  @Data
  private static class Candidate {

    private final InclusionDependency ind;
    private final boolean valid;

    static Candidate valid(final InclusionDependency ind) {
      return new Candidate(ind, true);
    }

    static Candidate invalid(final InclusionDependency ind) {
      return new Candidate(ind, false);
    }
  }

  private static InclusionDependency bSubsetOfA() {
    return InclusionDependencyBuilder
        .dependent().column("tableB", "B")
        .referenced().column("tableA", "A")
        .build();
  }

  private static InclusionDependency aSubsetOfB() {
    return InclusionDependencyBuilder
        .dependent().column("tableA", "A")
        .referenced().column("tableB", "B")
        .build();
  }

  private static InclusionDependency abSubsetOfCD() {
    return InclusionDependencyBuilder
        .dependent()
        .column("tableA", "A")
        .column("tableB", "B")
        .referenced()
        .column("tableC", "C")
        .column("tableD", "D")
        .build();
  }

  private static InclusionDependency cdSubsetOfAB() {
    return InclusionDependencyBuilder
        .dependent()
        .column("tableC", "C")
        .column("tableD", "D")
        .referenced()
        .column("tableA", "A")
        .column("tableB", "B")
        .build();
  }

  private static InclusionDependency eSubsetOfA() {
    return InclusionDependencyBuilder
        .dependent().column("tableE", "E")
        .referenced().column("tableA", "A")
        .build();
  }

  private static InclusionDependency aSubsetOfE() {
    return InclusionDependencyBuilder
        .dependent().column("tableA", "A")
        .referenced().column("tableE", "E")
        .build();
  }
}