package de.metanome.algorithms.zigzag;


import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import de.metanome.validation.ErrorMarginValidationResult;
import de.metanome.validation.ValidationStrategy;
import de.metanome.validation.ValidationStrategyFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class Zigzag {

  private int dbChecks = 0;

  private final ZigzagConfiguration configuration;
  private ValidationStrategy validationStrategy;
  private final ValidationStrategyFactory validationStrategyFactory;

  private int currentLevel;
  private Set<InclusionDependency> satisfiedINDs;
  private Set<InclusionDependency> unsatisfiedINDs;

  /* Keeps track of dependant-referenced relationship of the unary INDs
   * Only the dependant ColumnIdentifiers are used to express an IND
   * And are stored as Set<ColumnIdentifier>
  */
  private Map<ColumnIdentifier, ColumnIdentifier> unaryIndMap;

  public Zigzag(ZigzagConfiguration configuration) {
    this.configuration = configuration;
    currentLevel = configuration.getK();
    satisfiedINDs = new HashSet<>();
    unsatisfiedINDs = new HashSet<>();
    unaryIndMap = convertUnaryINDsToMap(configuration.getUnaryInds());
    validationStrategyFactory = new ValidationStrategyFactory();
  }

  public void execute() throws AlgorithmExecutionException {
    if(configuration.getUnaryInds().isEmpty()) {
      return;
    }

    validationStrategy = validationStrategyFactory
        .forDatabase(configuration.getValidationParameters());

    initialCandidateCheck(configuration.getK());

    Set<Set<ColumnIdentifier>> positiveBorder = indToNodes(satisfiedINDs); // Bd+(I)
    System.out.println("Postive Border: " + positiveBorder);
    Set<Set<ColumnIdentifier>> negativeBorder = indToNodes(unsatisfiedINDs); // Bd-(I)
    System.out.println("Negative Border: " + negativeBorder);

    Set<Set<ColumnIdentifier>> optimisticBorder = calculateOptimisticBorder(new HashSet<>(unsatisfiedINDs)); // Bd+(I opt)
    System.out.println("First optimistic Border: " + optimisticBorder);

    while(!isOptimisticBorderFinal(optimisticBorder, positiveBorder)) {
      Set<Set<ColumnIdentifier>> possibleSmallerINDs = new HashSet<>(); // optDI, all g3' < epsilon
      Set<Set<ColumnIdentifier>> pessDI = new HashSet<>(); // pessDI, all g3' > epsilon

      for(Set<ColumnIdentifier> ind : optimisticBorder) {
        double errorMargin = g3(nodeToInd(ind));
        if(errorMargin == 0.0) {
          positiveBorder.add(ind);
        } else {
          negativeBorder.add(ind);
          if(errorMargin <= configuration.getEpsilon() && ind.size() > currentLevel + 1) {
            possibleSmallerINDs.add(ind);
          } else {
            pessDI.add(ind);
          }
        }
      }
      while(!possibleSmallerINDs.isEmpty()) {
        Set<Set<ColumnIdentifier>> candidatesBelowOptimisticBorder = generalizeSet(possibleSmallerINDs);
        for(Set<ColumnIdentifier> indNode : candidatesBelowOptimisticBorder) {
          if(isIND(nodeToInd(indNode))) {
            positiveBorder.add(indNode);
            candidatesBelowOptimisticBorder.remove(indNode);
          } else {
            negativeBorder.add(indNode);
          }
        }
        possibleSmallerINDs = candidatesBelowOptimisticBorder;
      }
      positiveBorder = removeGeneralizations(positiveBorder);

      Set<Set<ColumnIdentifier>> candidatesOnNextLevel = getCandidatesOnNextLevel(pessDI); // C(k+1)
      for(Set<ColumnIdentifier> indNode : candidatesOnNextLevel) {
        // check if positiveBorder already satisfies ind before calling the database
        if(isSatisfiedByBorder(indNode, positiveBorder) || isIND(nodeToInd(indNode))) {
          positiveBorder.add(indNode);
        } else {
          negativeBorder.add(indNode);
        }
      }
      // remove unnecessary generalizations/specializations
      positiveBorder = removeGeneralizations(positiveBorder);
      negativeBorder = removeSpecializations(negativeBorder);

      currentLevel += 1;
      optimisticBorder = calculateOptimisticBorder(negativeBorder.stream()
          .map(this::nodeToInd)
          .collect(Collectors.toSet()));
    }
    satisfiedINDs = positiveBorder.stream()
        .flatMap(ind -> Sets.powerSet(ind).stream())
        .map(this::nodeToInd)
        .collect(Collectors.toSet());
    collectResults();

    validationStrategy.close();
    System.out.println("Total DB Checks: " + dbChecks);
  }

  private void collectResults() throws CouldNotReceiveResultException, ColumnNameMismatchException {
    for (InclusionDependency ind : satisfiedINDs) {
      configuration.getResultReceiver().receiveResult(ind);
    }
  }

  private boolean isSatisfiedByBorder(Set<ColumnIdentifier> ind,
      Set<Set<ColumnIdentifier>> positiveBorder) {
    for (Set<ColumnIdentifier> borderInds : positiveBorder) {
      if (borderInds.containsAll(ind)) {
        return true;
      }
    }
    return false;
  }

  private Set<Set<ColumnIdentifier>> getCandidatesOnNextLevel(Set<Set<ColumnIdentifier>> pessDI) {
    int nextLevel = currentLevel + 1;
    Set<Set<ColumnIdentifier>> generalizedINDs = pessDI.stream()
        .map(Sets::powerSet)
        .flatMap(Collection::stream)
        .filter(x -> x.size() == nextLevel)
        .collect(Collectors.toSet());
    /*for(Set<ColumnIdentifier> indNode : pessDI) {
      Set<Set<ColumnIdentifier>> powerSet = new HashSet<>(Sets.powerSet(indNode));
      powerSet.removeIf(x -> x.size() != nextLevel);
      generalizedINDs.addAll(powerSet);
    }*/
    return generalizedINDs;
  }

  private Set<Set<ColumnIdentifier>> generalizeSet(Set<Set<ColumnIdentifier>> possibleSmallerIND) {
    Set<Set<ColumnIdentifier>> generalizedINDs = new HashSet<>();
    for(Set<ColumnIdentifier> indNode : possibleSmallerIND) {
      Set<Set<ColumnIdentifier>> powerSet = new HashSet<>(Sets.powerSet(indNode));
      powerSet.removeIf(x -> x.size() != indNode.size() - 1);
      generalizedINDs.addAll(powerSet);
    }
    return generalizedINDs;
  }

  private boolean isOptimisticBorderFinal(Set<Set<ColumnIdentifier>> optimisticBorder, Set<Set<ColumnIdentifier>> positiveBorder) {
    System.out.println("Optimistic border: " + optimisticBorder);
    System.out.println("Positive border: " + positiveBorder);
    optimisticBorder.removeAll(positiveBorder);
    return optimisticBorder.isEmpty();
  }

  public Set<Set<ColumnIdentifier>> calculateOptimisticBorder(Set<InclusionDependency> unsatisfiedINDs) {
    Set<Set<ColumnIdentifier>> solution = new HashSet<>();
    Set<Set<ColumnIdentifier>> unsatisfiedNodes = indToNodes(unsatisfiedINDs);
    for (Set<ColumnIdentifier> head : unsatisfiedNodes) {
      Set<Set<ColumnIdentifier>> unpackedHead = head.stream().map(Sets::newHashSet).collect(Collectors.toSet());
      if(solution.size() == 0) {
        solution = unpackedHead;
      } else {
        Set<Set<ColumnIdentifier>> newSolution = unpackCartesianProduct(Sets.cartesianProduct(solution,unpackedHead));
        solution = removeSpecializations(newSolution);
      }
    }
    System.out.println("Non inverted: " + solution);
    return solution.stream().map(this::invertIND).collect(Collectors.toSet());
  }

  private Set<Set<ColumnIdentifier>> unpackCartesianProduct(Set<List<Set<ColumnIdentifier>>> x) {
    return x.stream()
        .map(list -> Sets.union(list.get(0), list.get(1)))
        .collect(Collectors.toSet());
  }

  private Set<Set<ColumnIdentifier>> indToNodes(Set<InclusionDependency> inds) {
    return inds.stream()
        .map(ind -> new HashSet<>(ind.getDependant().getColumnIdentifiers()))
        .collect(Collectors.toSet());
  }

  private InclusionDependency nodeToInd(Set<ColumnIdentifier> indNode) {
    List<ColumnIdentifier> depList = new ArrayList<>();
    List<ColumnIdentifier> refList = new ArrayList<>();
    for (ColumnIdentifier depId : indNode) {
      depList.add(depId);
      refList.add(unaryIndMap.get(depId));
    }
    ColumnPermutation dep = new ColumnPermutation();
    dep.setColumnIdentifiers(depList);
    ColumnPermutation ref = new ColumnPermutation();
    ref.setColumnIdentifiers(refList);
    return new InclusionDependency(dep, ref);
  }

  private Map<ColumnIdentifier,ColumnIdentifier> convertUnaryINDsToMap(Set<InclusionDependency> unaryINDs) {
    System.out.println("Unary INDs: " + unaryINDs);
    Map<ColumnIdentifier,ColumnIdentifier> uINDs = new HashMap<>();
    for (InclusionDependency ind : unaryINDs) {
      for (int i = 0; i < ind.getDependant().getColumnIdentifiers().size(); i++) {
        ColumnIdentifier dep = ind.getDependant().getColumnIdentifiers().get(i);
        ColumnIdentifier ref = ind.getReferenced().getColumnIdentifiers().get(i);
        // filters out columns in the same table
        if(notInTheSameTable(dep, ref)) {
          uINDs.put(dep, ref);
        }
      }
    }
    return uINDs;
  }

  private boolean notInTheSameTable(ColumnIdentifier dep, ColumnIdentifier ref) {
    return !dep.getTableIdentifier().equals(ref.getTableIdentifier());
  }

  // Zigzag only needs to check one side for this, so use dependant
  private Set<Set<ColumnIdentifier>> removeSpecializations(Set<Set<ColumnIdentifier>> solution) {
    Set<Set<ColumnIdentifier>> minimalSolution = new HashSet<>(solution);
    for (Set<ColumnIdentifier> ind1 : solution) {
      for (Set<ColumnIdentifier> ind2 : solution) {
        if(isSpecialization(ind1, ind2)) {
          minimalSolution.remove(ind1);
        }
      }
    }
    return minimalSolution;
  }

  private Set<Set<ColumnIdentifier>> removeGeneralizations(Set<Set<ColumnIdentifier>> solution) {
    Set<Set<ColumnIdentifier>> maximalSolution = new HashSet<>(solution);
    for (Set<ColumnIdentifier> ind1 : solution) {
      for (Set<ColumnIdentifier> ind2 : solution) {
        if(isGeneralization(ind1, ind2)) {
          maximalSolution.remove(ind1);
        }
      }
    }
    return maximalSolution;
  }

  private Set<ColumnIdentifier> invertIND(Set<ColumnIdentifier> ind) {
    Set<ColumnIdentifier> invertedIND = new HashSet<>(unaryIndMap.keySet());
    for(ColumnIdentifier depId : ind) {
      invertedIND.remove(depId);
    }
    return invertedIND;
  }

  private boolean isSpecialization(Set<ColumnIdentifier> specialization, Set<ColumnIdentifier> generalization) {
    return specialization.size() > generalization.size()
        && specialization.containsAll(generalization);
  }

  private boolean isGeneralization(Set<ColumnIdentifier> generalization, Set<ColumnIdentifier> specialization) {
    return generalization.size() < specialization.size()
        && specialization.containsAll(generalization);
  }

  private double g3(InclusionDependency ind) {
    if(hasMultipleTablesPerSide(ind)) {
      System.out.println("INVALID. IND has multiple tables on one side: " + ind);
      return configuration.getEpsilon() + 1;
    }
    // INVALID if it has duplicates
    // only check for referenced, since dependant is held in a set
    if(hasDuplicate(ind.getReferenced().getColumnIdentifiers())) {
      return configuration.getEpsilon() + 1;
    }
    dbChecks++;
    System.out.println("G3 Checking: " + ind);
    return ((ErrorMarginValidationResult) validationStrategy.validate(ind)).getErrorMargin();
  }

  // equivalent to d |= i in paper
  private boolean isIND(InclusionDependency ind) {
    if(hasMultipleTablesPerSide(ind)) {
      System.out.println("INVALID. IND has multiple tables on one side: " + ind);
      return false;
    }
    // INVALID if it has duplicates
    // only check for referenced, since dependant is held in a set
    if(hasDuplicate(ind.getReferenced().getColumnIdentifiers())) {
      return false;
    }
    dbChecks++;
    System.out.println("Checking: " + ind);
    return validationStrategy.validate(ind).isValid();
  }

  private <T> boolean hasDuplicate(Iterable<T> all) {
    Set<T> set = new HashSet<T>();
    // Set#add returns false if the set does not change, which
    // indicates that a duplicate element has been added.
    for (T each: all) if (!set.add(each)) return true;
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
    return depTableCount != 1 || refTableCount != 1;
  }

  private void initialCandidateCheck(int k) {
    for(int i = 2; i <= k; i++) {
      Set<InclusionDependency> candidates = generateCandidatesForLevel(i);
      for(InclusionDependency ind : candidates) {
        System.out.println("IND to check: " + ind);
        if(isIND(ind)) {
          satisfiedINDs.add(ind);
          System.out.println("New satisfied IND: " + ind);
        } else {
          unsatisfiedINDs.add(ind);
          System.out.println("New unsatisfied IND: " + ind);
        }
      }
    }
  }

  private Set<InclusionDependency> generateCandidatesForLevel(int i) {
    Set<ColumnIdentifier> unaryIndNodes = new HashSet<>();
    for(Entry<ColumnIdentifier, ColumnIdentifier> entry : unaryIndMap.entrySet()) {
      if(!entry.getKey().getTableIdentifier().equals(entry.getValue().getTableIdentifier())) {
        unaryIndNodes.add(entry.getKey());
      }
    }
    System.out.println("Combinations for level " + i + " : " + Sets.combinations(unaryIndNodes,i).toString());
    return Sets.combinations(unaryIndNodes,i).stream()
        .map(this::nodeToInd)
        .collect(Collectors.toSet());
  }
}