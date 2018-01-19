package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Mind {

  private Configuration configuration;
  private ValidationStrategy validationStrategy;
  private List<String> relationNames;

  private List<TableInfo> tables;
  private final List<ColumnPermutation[]> results = new ArrayList<>();

  private final TableInfoFactory tableInfoFactory;
  private final ValidationStrategyFactory validationStrategyFactory;

  public Mind() {
    tableInfoFactory = new TableInfoFactory();
    validationStrategyFactory = new ValidationStrategyFactory();
  }

  public void execute(final Configuration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;
    validationStrategy = validationStrategyFactory
        .forDatabase(configuration.getValidationParameters());

    initialize();
    List<ColumnPermutation[]> candidates = genLevel1Candidates();

    int depth = 1;

    while (!candidates.isEmpty() && (configuration.getMaxDepth() < 0 || depth <= configuration.getMaxDepth())) {
      final List<ColumnPermutation[]> inds = new ArrayList<>();
      for (final ColumnPermutation[] candidate : candidates) {
        final ColumnPermutation lhs = candidate[0];
        final ColumnPermutation rhs = candidate[1];
        if (isInd(lhs, rhs, depth)) {
          final InclusionDependency ind = new InclusionDependency(lhs, rhs);
          configuration.getResultReceiver().receiveResult(ind);
          results.add(candidate);
          inds.add(candidate);
        }
      }
      depth++;
      candidates = genNextLevelCandidates(inds);
    }

    validationStrategy.close();
  }

  private List<ColumnPermutation[]> genLevel1Candidates() {
    final List<ColumnIdentifier> attributes = new ArrayList<>();
    final List<ColumnPermutation[]> candidates = new ArrayList<>();
    for (final TableInfo table : tables) {
      for (String column : table.getColumnNames()) {
        attributes.add(new ColumnIdentifier(table.getTableName(), column));
      }
    }

    for (final ColumnIdentifier lhs : attributes) {
      for (final ColumnIdentifier rhs : attributes) {
        if (!lhs.equals(rhs)) {
          final ColumnPermutation[] candidate = {new ColumnPermutation(lhs),
              new ColumnPermutation(rhs)};
          candidates.add(candidate);
        }
      }
    }

    return candidates;
  }

  private List<ColumnPermutation[]> genNextLevelCandidates(
      final List<ColumnPermutation[]> previous) {
    final List<ColumnPermutation[]> candidates = new ArrayList<>();
    for (int index1 = 0; index1 < previous.size(); index1++) {
      for (int index2 = index1 + 1; index2 < previous.size(); index2++) {

        if (samePrefix(previous.get(index1)[0], previous.get(index2)[0]) &&
            samePrefix(previous.get(index1)[1], previous.get(index2)[1]) &&
            sameTable(previous.get(index1), previous.get(index2))) {

          final ColumnPermutation[] candidate = {
              new ColumnPermutation(),
              new ColumnPermutation()};
          final List<ColumnIdentifier> colIdsLHS = new ArrayList<>(
              previous.get(index1)[0].getColumnIdentifiers());
          List<ColumnIdentifier> index2Ids = previous.get(index2)[0].getColumnIdentifiers();
          colIdsLHS.add(index2Ids.get(index2Ids.size() - 1));

          final List<ColumnIdentifier> colIdsRHS = new ArrayList<>(
              previous.get(index1)[1].getColumnIdentifiers());

          index2Ids = previous.get(index2)[1].getColumnIdentifiers();
          colIdsRHS.add(index2Ids.get(index2Ids.size() - 1));

          candidate[0].setColumnIdentifiers(colIdsLHS);
          candidate[1].setColumnIdentifiers(colIdsRHS);
          if (notToPrune(candidate) && isNotDoublon(candidate)) {
            candidates.add(candidate);
          }
        }
      }
    }
    return candidates;
  }

  private boolean notToPrune(final ColumnPermutation[] candidate) {
    for (int index = 0; index < candidate[0].getColumnIdentifiers().size(); index++) {
      final List<ColumnIdentifier> lhs = new ArrayList<>(
          candidate[0].getColumnIdentifiers());
      final List<ColumnIdentifier> rhs = new ArrayList<>(
          candidate[1].getColumnIdentifiers());
      lhs.remove(index);
      rhs.remove(index);
      boolean is_included = false;
      for (final ColumnPermutation[] ind : results) {
        if (ind[0].getColumnIdentifiers().equals(lhs) && ind[1].getColumnIdentifiers()
            .equals(rhs)) {
          is_included = true;
          break;
        }
      }
      if (!is_included) {
        return false;
      }
    }
    return true;
  }

  private boolean isNotDoublon(final ColumnPermutation[] candidate) {
    final List<ColumnIdentifier> colIdsLHS = candidate[0].getColumnIdentifiers();
    final List<ColumnIdentifier> colIdsRHS = candidate[1].getColumnIdentifiers();
    for (final ColumnIdentifier colId : colIdsLHS) {
      if (Collections.frequency(colIdsLHS, colId) > 1
          || Collections.frequency(colIdsRHS, colId) > 0) {
        return false;
      }
    }
    for (final ColumnIdentifier colId : colIdsRHS) {
      if (Collections.frequency(colIdsLHS, colId) > 0
          || Collections.frequency(colIdsRHS, colId) > 1) {
        return false;
      }
    }

    return true;
  }

  private boolean sameTable(final ColumnPermutation[] columnPermutation1,
      final ColumnPermutation[] columnPermutation2) {
    return (columnPermutation1[0].getColumnIdentifiers().get(0).getTableIdentifier().equals(
        columnPermutation2[0].getColumnIdentifiers().get(0).getTableIdentifier())) &&
        (columnPermutation1[1].getColumnIdentifiers().get(0).getTableIdentifier().equals(
            columnPermutation2[1].getColumnIdentifiers().get(0).getTableIdentifier()));
  }

  private boolean samePrefix(final ColumnPermutation columnPermutation1,
      final ColumnPermutation columnPermutation2) {
    final List<ColumnIdentifier> col1Identifiers = columnPermutation1.getColumnIdentifiers();
    final List<ColumnIdentifier> col2Identifiers = columnPermutation2.getColumnIdentifiers();

    for (int index = 0; index < col1Identifiers.size() - 1; index++) {
      if (!col1Identifiers.get(index).equals(col2Identifiers.get(index))) {
        return false;
      }
    }
    return true;
  }

  private void initialize() throws InputGenerationException, AlgorithmConfigurationException {
    this.relationNames = new ArrayList<>();

    tables = tableInfoFactory
        .createFromTableInputs(configuration.getTableInputGenerators());

    for (final TableInfo table : tables) {
      this.relationNames.add(table.getTableName());
    }
  }

  private boolean isInd(final ColumnPermutation lhs, final ColumnPermutation rhs, int depth) {
    return validationStrategy.validate(new InclusionDependency(lhs, rhs)).isValid();
  }
}
