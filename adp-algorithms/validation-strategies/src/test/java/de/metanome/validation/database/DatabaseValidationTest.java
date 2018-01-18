package de.metanome.validation.database;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.generators.Generate.enumValues;
import static org.quicktheories.generators.Generate.pick;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.validation.ValidationResult;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.Data;
import org.jooq.DSLContext;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quicktheories.WithQuickTheories;

class DatabaseValidationTest implements WithQuickTheories {

  private DSLContext context;

  private final Queries queries = new Queries();

  @BeforeEach
  void setUp() throws Exception {
    context = Helper.createInMemoryContext();

    context.createTable("tableA")
        .column("A", SQLDataType.INTEGER)
        .column("B", SQLDataType.INTEGER)
        .execute();

    context.createTable("tableB")
        .column("C", SQLDataType.INTEGER)
        .column("D", SQLDataType.INTEGER)
        .column("E", SQLDataType.INTEGER)
        .execute();

    Helper.loadCsv(context, "tableA.csv", "tableA");
    Helper.loadCsv(context, "tableB.csv", "tableB");
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
        .dependent().column("tableA", "B")
        .referenced().column("tableA", "A")
        .build();
  }

  private static InclusionDependency aSubsetOfB() {
    return InclusionDependencyBuilder
        .dependent().column("tableA", "A")
        .referenced().column("tableA", "B")
        .build();
  }

  private static InclusionDependency abSubsetOfCD() {
    return InclusionDependencyBuilder
        .dependent()
        .column("tableA", "A")
        .column("tableA", "B")
        .referenced()
        .column("tableB", "C")
        .column("tableB", "D")
        .build();
  }

  private static InclusionDependency cdSubsetOfAB() {
    return InclusionDependencyBuilder
        .dependent()
        .column("tableB", "C")
        .column("tableB", "D")
        .referenced()
        .column("tableA", "A")
        .column("tableA", "B")
        .build();
  }

  private static InclusionDependency eSubsetOfA() {
    return InclusionDependencyBuilder
        .dependent().column("tableB", "E")
        .referenced().column("tableA", "A")
        .build();
  }

  private static InclusionDependency aSubsetOfE() {
    return InclusionDependencyBuilder
        .dependent().column("tableA", "A")
        .referenced().column("tableB", "E")
        .build();
  }
}