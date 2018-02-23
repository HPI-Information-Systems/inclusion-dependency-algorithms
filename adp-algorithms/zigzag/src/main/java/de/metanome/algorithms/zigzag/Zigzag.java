package de.metanome.algorithms.zigzag;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import de.metanome.input.ind.InclusionDependencyInput;
import de.metanome.input.ind.InclusionDependencyInputGenerator;
import de.metanome.validation.ErrorMarginValidationResult;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;
import de.metanome.util.InclusionDependencyUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javafx.util.Pair;

import static java.lang.String.format;

public class Zigzag {

  private final static Logger log = Logger.getLogger(Zigzag.class.getName());

  private final ZigzagConfiguration config;
  private final ValidationStrategy validationStrategy;
  private final InclusionDependencyInput inclusionDependencyInput;

  private final Set<InclusionDependency> satisfiedINDs;
  private final Set<InclusionDependency> unsatisfiedINDs;
  private Set<InclusionDependency> unaryINDs;
  private int currentLevel;
  private Set<InclusionDependency> results;
  private int dbChecks = 0;

  Zigzag(ZigzagConfiguration config) {
    this.config = config;
    currentLevel = config.getStartK();

    satisfiedINDs = new HashSet<>();
    unsatisfiedINDs = new HashSet<>();
    results = new HashSet<>();

    validationStrategy = new ValidationStrategyFactory()
        .forDatabase(config.getValidationParameters());
    inclusionDependencyInput = new InclusionDependencyInputGenerator()
        .get(config.getInclusionDependencyParameters());
  }

  public void execute() throws AlgorithmExecutionException {
    List<InclusionDependency> allInputINDs = inclusionDependencyInput.execute();

    Map<Pair<String, String>, List<InclusionDependency>> tablesPairToINDList = allInputINDs.stream()
        .collect(Collectors.groupingBy(
            ind -> new Pair<>(ind.getDependant().getColumnIdentifiers().get(0).getTableIdentifier(),
                ind.getDependant().getColumnIdentifiers().get(0).getTableIdentifier())));

    for (List<InclusionDependency> split : tablesPairToINDList.values()) {
      partialExecute(split);
      currentLevel = config.getStartK();
      satisfiedINDs.clear();
      unsatisfiedINDs.clear();
    }

    commitResults();
    validationStrategy.close();
    log.info(format("Total DB checks: %s", dbChecks));
  }

  private void partialExecute(List<InclusionDependency> inputINDs) {
    unaryINDs = getUnaryINDs(inputINDs);
    checkKaryInputInds(config.getStartK(), inputINDs);
    Set<Set<InclusionDependency>> positiveBorder = indToNodes(satisfiedINDs); // Bd+(I)
    log.info(format("Positive Border: %s", positiveBorder));
    Set<Set<InclusionDependency>> negativeBorder = indToNodes(unsatisfiedINDs); // Bd-(I)
    log.info(format("Negative Border: %s", negativeBorder));

    Set<Set<InclusionDependency>> optimisticBorder = calculateOptimisticBorder(
        new HashSet<>(unsatisfiedINDs)); // Bd+(I opt)
    log.info(format("First optimistic border %s", optimisticBorder));

    while (!isOptimisticBorderFinal(optimisticBorder, positiveBorder)) {
      Set<Set<InclusionDependency>> possibleSmallerINDs = new HashSet<>(); // optDI, all g3' < epsilon
      Set<Set<InclusionDependency>> pessDI = new HashSet<>(); // pessDI, all g3' > epsilon

      for (Set<InclusionDependency> ind : optimisticBorder) {
        double errorMargin = g3(nodeToInd(ind));
        if (errorMargin == 0.0) {
          positiveBorder.add(ind);
        } else {
          negativeBorder.add(ind);
          if (errorMargin <= config.getEpsilon()
              && ind.size() > currentLevel + 1) {
            possibleSmallerINDs.add(ind);
          } else {
            pessDI.add(ind);
          }
        }
      }

      while (!possibleSmallerINDs.isEmpty()) {
        Set<Set<InclusionDependency>> candidatesBelowOptimisticBorder = generalizeSet(
            possibleSmallerINDs);
        for (Iterator<Set<InclusionDependency>> iter = candidatesBelowOptimisticBorder.iterator();
            iter.hasNext(); ) {
          Set<InclusionDependency> indNode = iter.next();
          if (isIND(nodeToInd(indNode))) {
            positiveBorder.add(indNode);
            iter.remove();
          } else {
            negativeBorder.add(indNode);
          }
        }
        possibleSmallerINDs = candidatesBelowOptimisticBorder;
      }
      positiveBorder = removeGeneralizations(positiveBorder);

      Set<Set<InclusionDependency>> candidatesOnNextLevel = getCandidatesOnNextLevel(
          pessDI); // C(k+1)
      for (Set<InclusionDependency> indNode : candidatesOnNextLevel) {
        // check if positiveBorder already satisfies ind before calling the database
        if (isSatisfiedByBorder(indNode, positiveBorder) || isIND(nodeToInd(indNode))) {
          positiveBorder.add(indNode);
        } else {
          negativeBorder.add(indNode);
        }
      }
      // remove unnecessary generalizations/specializations
      positiveBorder = removeGeneralizations(positiveBorder);
      negativeBorder = removeSpecializations(negativeBorder);

      currentLevel += 1;
      log.info(format("Calculating next border for level: %d", currentLevel));
      optimisticBorder = calculateOptimisticBorder(negativeBorder.stream()
          .map(this::nodeToInd)
          .collect(Collectors.toSet()));
    }
    addResultFromPositiveBorder(positiveBorder);
  }

  private void addResultFromPositiveBorder(Set<Set<InclusionDependency>> positiveBorder) {
    Set<InclusionDependency> satisfiedInds = positiveBorder.stream()
        .flatMap(ind -> Sets.powerSet(ind).stream())
        .filter(ind -> !ind.isEmpty())
        .map(this::nodeToInd)
        .collect(Collectors.toSet());
    // Add unary INDs in case some were pruned out earlier e.g. INDs in the same table
    satisfiedInds.addAll(unaryINDs);
    addToResultSet(satisfiedInds);
  }

  private void addToResultSet(Set<InclusionDependency> satisfiedInds) {
    results.addAll(satisfiedInds);
  }

  private void commitResults() throws ColumnNameMismatchException, CouldNotReceiveResultException {
    InclusionDependencyUtil util = new InclusionDependencyUtil();
    for (InclusionDependency satisfiedInd : util.getMax(results)) {
      config.getResultReceiver().receiveResult(satisfiedInd);
    }
  }

  private boolean isSatisfiedByBorder(Set<InclusionDependency> ind,
      Set<Set<InclusionDependency>> positiveBorder) {
    for (Set<InclusionDependency> borderInds : positiveBorder) {
      if (borderInds.containsAll(ind)) {
        return true;
      }
    }
    return false;
  }

  private Set<Set<InclusionDependency>> getCandidatesOnNextLevel(
      Set<Set<InclusionDependency>> pessDI) {
    int nextLevel = currentLevel + 1;
    return pessDI.stream()
        .filter(ind -> ind.size() >= nextLevel)
        .flatMap(indNode -> Sets.combinations(indNode, nextLevel).stream())
        .collect(Collectors.toSet());
  }

  private Set<Set<InclusionDependency>> generalizeSet(
      Set<Set<InclusionDependency>> possibleSmallerIND) {
    return possibleSmallerIND.stream()
        .flatMap(indNode -> Sets.combinations(indNode, indNode.size() - 1).stream())
        .collect(Collectors.toSet());
  }

  private boolean isOptimisticBorderFinal(
      Set<Set<InclusionDependency>> optimisticBorder,
      Set<Set<InclusionDependency>> positiveBorder) {
    optimisticBorder.removeAll(positiveBorder);
    return optimisticBorder.isEmpty();
  }

  // Currently very inefficient for many unsatisfied INDs, e.g. when generating many invalid ones
  // This stems from combining all of them with cartesianProducts
  // This algorithm is from the HPI data profiling lecture from SS17
  private Set<Set<InclusionDependency>> calculateOptimisticBorder(
      Set<InclusionDependency> unsatisfiedINDs) {
    Set<Set<InclusionDependency>> solution = new HashSet<>();
    Set<Set<InclusionDependency>> unsatisfiedNodes = indToNodes(unsatisfiedINDs);
    for (Set<InclusionDependency> head : unsatisfiedNodes) {
      log.info(format("Adding to cartesian product: %s", head));
      Set<Set<InclusionDependency>> unpackedHead = head.stream()
          .map(Sets::newHashSet)
          .collect(Collectors.toSet());
      if (solution.size() == 0) {
        solution = unpackedHead;
      } else {
        solution = unpackCartesianProduct(solution, unpackedHead);
        solution = removeSpecializations(solution);
      }
    }
    return solution.stream().map(this::invertIND).collect(Collectors.toSet());
  }

  private Set<Set<InclusionDependency>> unpackCartesianProduct(
      Set<Set<InclusionDependency>> solution, Set<Set<InclusionDependency>> head) {
    return Sets.cartesianProduct(ImmutableList.of(solution, head)).stream()
        .map(list -> Sets.union(list.get(0), list.get(1)))
        .collect(Collectors.toSet());
  }

  // Converts one IND to a set of its contained unary INDs
  private Set<InclusionDependency> indToNode(InclusionDependency ind) {
    Set<InclusionDependency> indNode = new HashSet<>();
    List<ColumnIdentifier> dep = ind.getDependant().getColumnIdentifiers();
    List<ColumnIdentifier> ref = ind.getReferenced().getColumnIdentifiers();
    for (int i = 0; i < dep.size(); i++) {
      indNode.add(makeUnaryInd(dep.get(i), ref.get(i)));
    }
    return indNode;
  }

  private Set<Set<InclusionDependency>> indToNodes(Set<InclusionDependency> inds) {
    return inds.stream()
        .map(this::indToNode)
        .collect(Collectors.toSet());
  }

  private InclusionDependency makeUnaryInd(ColumnIdentifier dep, ColumnIdentifier ref) {
    return new InclusionDependency(new ColumnPermutation(dep), new ColumnPermutation(ref));
  }

  private InclusionDependency nodeToInd(Set<InclusionDependency> indNode) {
    List<ColumnIdentifier> depList = new ArrayList<>();
    List<ColumnIdentifier> refList = new ArrayList<>();
    for (InclusionDependency unaryInd : indNode) {
      depList.add(unaryInd.getDependant().getColumnIdentifiers().get(0));
      refList.add(unaryInd.getReferenced().getColumnIdentifiers().get(0));
    }
    ColumnPermutation dep = new ColumnPermutation();
    dep.setColumnIdentifiers(depList);
    ColumnPermutation ref = new ColumnPermutation();
    ref.setColumnIdentifiers(refList);
    return new InclusionDependency(dep, ref);
  }

  private boolean isNotInTheSameTable(ColumnIdentifier dep, ColumnIdentifier ref) {
    return !dep.getTableIdentifier().equals(ref.getTableIdentifier());
  }

  private Set<Set<InclusionDependency>> removeSpecializations(
      Set<Set<InclusionDependency>> solution) {
    Set<Set<InclusionDependency>> minimalSolution = new HashSet<>(solution);
    for (Set<InclusionDependency> ind1 : solution) {
      for (Set<InclusionDependency> ind2 : solution) {
        if (isSpecialization(ind1, ind2)) {
          minimalSolution.remove(ind1);
        }
      }
    }
    return minimalSolution;
  }

  private Set<Set<InclusionDependency>> removeGeneralizations(
      Set<Set<InclusionDependency>> solution) {
    Set<Set<InclusionDependency>> maximalSolution = new HashSet<>(solution);
    for (Set<InclusionDependency> ind1 : solution) {
      for (Set<InclusionDependency> ind2 : solution) {
        if (isGeneralization(ind1, ind2)) {
          maximalSolution.remove(ind1);
        }
      }
    }
    return maximalSolution;
  }

  private Set<InclusionDependency> invertIND(Set<InclusionDependency> ind) {
    Set<InclusionDependency> invertedIND = new HashSet<>(unaryINDs);
    for (InclusionDependency unaryInd : ind) {
      invertedIND.remove(unaryInd);
    }
    return invertedIND;
  }

  private boolean isSpecialization(
      Set<InclusionDependency> specialization, Set<InclusionDependency> generalization) {
    return specialization.size() > generalization.size()
        && specialization.containsAll(generalization);
  }

  private boolean isGeneralization(
      Set<InclusionDependency> generalization, Set<InclusionDependency> specialization) {
    return generalization.size() < specialization.size()
        && specialization.containsAll(generalization);
  }

  private double g3(InclusionDependency ind) {
    if (hasMultipleTablesPerSide(ind)) {
      log.info(format("Invalid: IND has multiple tables on one side (IND: %s)", ind));
      return config.getEpsilon() + 1;
    }
    // INVALID if it has duplicates
    if (hasDuplicate(ind.getDependant().getColumnIdentifiers())
        || hasDuplicate(ind.getReferenced().getColumnIdentifiers())) {
      return config.getEpsilon() + 1;
    }
    dbChecks++;
    log.info(format("G3 checking: %s", ind));
    return ((ErrorMarginValidationResult) validationStrategy.validate(ind)).getErrorMargin();
  }

  // equivalent to d |= i in paper
  private boolean isIND(InclusionDependency ind) {
    if (hasMultipleTablesPerSide(ind)) {
      log.info(format("Invalid: IND has multiple tables on one side (IND: %s)", ind));
      return false;
    }
    // INVALID if it has duplicates
    if (hasDuplicate(ind.getDependant().getColumnIdentifiers())
        || hasDuplicate(ind.getReferenced().getColumnIdentifiers())) {
      return false;
    }
    dbChecks++;
    return validationStrategy.validate(ind).isValid();
  }

  private <T> boolean hasDuplicate(Iterable<T> all) {
    Set<T> set = new HashSet<>();
    // Set#add returns false if the set does not change, which
    // indicates that a duplicate element has been added.
    for (T each : all) {
      if (!set.add(each)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasMultipleTablesPerSide(InclusionDependency ind) {
    int depTableCount = ind.getDependant().getColumnIdentifiers().stream()
        .map(ColumnIdentifier::getTableIdentifier)
        .collect(Collectors.toSet())
        .size();
    int refTableCount = ind.getReferenced().getColumnIdentifiers().stream()
        .map(ColumnIdentifier::getTableIdentifier)
        .collect(Collectors.toSet())
        .size();
    return depTableCount > 1 || refTableCount > 1;
  }

  private void checkKaryInputInds(int k, List<InclusionDependency> inputINDs) {
    for (int i = 2; i <= k; i++) {
      Set<InclusionDependency> candidates = generateCandidatesForLevel(i);
      for (InclusionDependency ind : candidates) {
        if (indIsInInputINDs(ind, inputINDs)) {
          satisfiedINDs.add(ind);
          log.info(format("New satisfied IND: %s", ind));
        } else {
          unsatisfiedINDs.add(ind);
          log.info(format("New unsatisfied IND: %s", ind));
        }
      }
    }
  }

  private boolean indIsInInputINDs(InclusionDependency ind, List<InclusionDependency> inputINDs) {
    for (InclusionDependency inIND : inputINDs) {
      if (indToNode(inIND).equals(indToNode(ind))) {
        return true;
      }
    }
    return false;
  }

  private Set<InclusionDependency> generateCandidatesForLevel(int i) {
    // Filter out unary INDs from the same table to prevent generating tons of invalid INDs
    Set<InclusionDependency> unaryIndNodes = unaryINDs;
//        .stream()
//        .filter(ind -> isNotInTheSameTable(
//            ind.getDependant().getColumnIdentifiers().get(0),
//            ind.getReferenced().getColumnIdentifiers().get(0)))
//        .collect(Collectors.toSet());
    if (unaryIndNodes.size() < i) {
      return new HashSet<>();
    }
    Set<Set<InclusionDependency>> indsForLevel = Sets.combinations(unaryIndNodes, i);
    return indsForLevel.stream()
        .map(this::nodeToInd)
        .collect(Collectors.toSet());
  }

  private Set<InclusionDependency> getUnaryINDs(final Collection<InclusionDependency> inputINDs) {
    final Set<InclusionDependency> result = new HashSet<>();
    for (InclusionDependency ind : inputINDs) {
      if (ind.getDependant().getColumnIdentifiers().size() == 1) {
        final InclusionDependency IND = new InclusionDependency(ind.getDependant(),
            ind.getReferenced());
        result.add(IND);
      }
    }
    return result;
  }
}
