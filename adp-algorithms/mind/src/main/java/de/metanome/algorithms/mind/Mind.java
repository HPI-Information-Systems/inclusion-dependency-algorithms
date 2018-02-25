package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.input.ind.InclusionDependencyInput;
import de.metanome.input.ind.InclusionDependencyInputGenerator;
import de.metanome.util.InclusionDependencyUtil;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Mind {

  private Configuration configuration;
  private ValidationStrategy validationStrategy;

  private final List<ColumnPermutation[]> results = new ArrayList<>();

  private final InclusionDependencyInputGenerator inclusionDependencyInputGenerator;
  private final ValidationStrategyFactory validationStrategyFactory;
  private final InclusionDependencyUtil util;

  public Mind() {
    inclusionDependencyInputGenerator = new InclusionDependencyInputGenerator();
    validationStrategyFactory = new ValidationStrategyFactory();
    util = new InclusionDependencyUtil();
  }

  public void execute(final Configuration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;
    validationStrategy = validationStrategyFactory
        .forDatabase(configuration.getValidationParameters());

    List<ColumnPermutation[]> candidates = genNextLevelCandidates(genLevel1Candidates());

    int depth = 1;

    while (!candidates.isEmpty() && (configuration.getMaxDepth() < 0 || depth < configuration
        .getMaxDepth())) {
      final List<ColumnPermutation[]> inds = new ArrayList<>();
      for (final ColumnPermutation[] candidate : candidates) {
        final ColumnPermutation lhs = candidate[0];
        final ColumnPermutation rhs = candidate[1];
        if (isInd(lhs, rhs)) {
          results.add(candidate);
          inds.add(candidate);
        }
      }
      depth++;
      candidates = genNextLevelCandidates(inds);
    }

    receiveResult();
    validationStrategy.close();
  }


  private List<ColumnPermutation[]> genLevel1Candidates() throws AlgorithmExecutionException {
    final List<InclusionDependency> inds = retrieveInputInds();
    final List<ColumnPermutation[]> internal = toInternalRepresentation(inds);
    results.addAll(internal);
    return internal;
  }

  private List<InclusionDependency> retrieveInputInds() throws AlgorithmExecutionException {
    final InclusionDependencyInput input = inclusionDependencyInputGenerator
        .get(configuration.getInclusionDependencyParameters());
    return input.execute();
  }

  private void receiveResult() throws AlgorithmExecutionException {
    final List<InclusionDependency> inds = toExternalRepresentation(results);
    final List<InclusionDependency> toOutput =
        configuration.isOutputMaxInd() ? util.getMax(inds) : inds;

    for (final InclusionDependency ind : toOutput) {
      configuration.getResultReceiver().receiveResult(ind);
    }
  }

  private List<ColumnPermutation[]> toInternalRepresentation(final List<InclusionDependency> inds) {
    final List<ColumnPermutation[]> columnPermutations = new ArrayList<>(inds.size());
    for (final InclusionDependency ind : inds) {
      columnPermutations.add(new ColumnPermutation[]{ind.getDependant(), ind.getReferenced()});
    }
    return columnPermutations;
  }

  private List<InclusionDependency> toExternalRepresentation(
      final List<ColumnPermutation[]> permutations) {

    final List<InclusionDependency> inds = new ArrayList<>(permutations.size());
    for (final ColumnPermutation[] cp : permutations) {
      inds.add(new InclusionDependency(cp[0], cp[1]));
    }
    return inds;
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

  private boolean isInd(final ColumnPermutation lhs, final ColumnPermutation rhs) {
    return validationStrategy.validate(new InclusionDependency(lhs, rhs)).isValid();
  }
}
