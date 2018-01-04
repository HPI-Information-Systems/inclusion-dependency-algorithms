package de.metanome.algorithms.zigzag;


import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.zigzag.configuration.ZigzagConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.util.Pair;

public class Zigzag {

  private final ZigzagConfiguration configuration;
  private int currentLevel;
  private Set<InclusionDependency> satisfiedINDs;
  private Set<InclusionDependency> unsatisfiedINDs;
  private Set<InclusionDependency> unaryINDs;

  /* Keeps track of dependant-referenced relationship of the unary INDs
   * In the hypergraph only dependant ColumnIdentifier is used to express an IND
   * And are stored as Set<ColumnIdentifier>
  */
  Map<ColumnIdentifier, ColumnIdentifier> dependantToReferenced;

  public Zigzag(ZigzagConfiguration configuration) {
    this.configuration = configuration;
    unaryINDs = calculateUnaryInclusionDependencies();
    currentLevel = configuration.getK();
  }

  public void execute() throws AlgorithmExecutionException {
    initialCandidateCheck(configuration.getK());

    dependantToReferenced = convertUnaryINDsToMap(calculateUnaryInclusionDependencies());

    Set<Set<ColumnIdentifier>> positiveBorder = indToNodes(satisfiedINDs); // Bd+(I)
    Set<Set<ColumnIdentifier>> negativeBorder = indToNodes(unsatisfiedINDs); // Bd-(I)

    Set<Set<ColumnIdentifier>> optimisticBorder = calculateOptimisticBorder(new HashSet<>(unsatisfiedINDs)); // Bd+(I opt)

    while(!isOptimisticBorderFinal(optimisticBorder, positiveBorder)) {
      Set<Set<ColumnIdentifier>> possibleSmallerIND = new HashSet<>(); // optDI, all g3' < epsilon
      Set<Set<ColumnIdentifier>> pessDI = new HashSet<>(); // pessDI, all g3' > epsilon

      for(Set<ColumnIdentifier> ind : optimisticBorder) {
        int g3Val = g3(nodeToInd(ind));
        if(g3Val == 0) {
          System.out.println(positiveBorder);
          positiveBorder.add(ind);
          System.out.println(positiveBorder);
          positiveBorder = removeGeneralizations(positiveBorder);
          System.out.println(positiveBorder);
        } else {
          negativeBorder.add(ind);
          if(g3Val <= configuration.getEpsilon() && ind.size() > currentLevel + 1) {
            possibleSmallerIND.add(ind);
          } else {
            pessDI.add(ind);
          }
        }
      }
      while(!possibleSmallerIND.isEmpty()) {
        // TODO INDs which generalize possibleSmallerINDs one level below
        Set<Set<ColumnIdentifier>> candidates = generalizeSet(possibleSmallerIND);
        for(Set<ColumnIdentifier> indNode : candidates) {
          if(isIND(nodeToInd(indNode))) {
            positiveBorder.add(indNode);
            positiveBorder = removeGeneralizations(positiveBorder);
            candidates.remove(indNode);
          } else {
            negativeBorder.add(indNode);
          }
        }
        possibleSmallerIND = candidates;
      }
      // TODO check INDs which generalize pessDI on k+1 level
      Set<Set<ColumnIdentifier>> C = pessDI;
      for(Set<ColumnIdentifier> ind : C) {
        if(positiveBorder.contains(ind) || isIND(nodeToInd(ind))) {
          positiveBorder.add(ind);
          positiveBorder = removeGeneralizations(positiveBorder);
        } else {
          negativeBorder.add(ind);
          negativeBorder = removeSpecializations(negativeBorder);
        }
      }

      currentLevel += 1;
      optimisticBorder = calculateOptimisticBorder(new HashSet<>(unsatisfiedINDs));
    }
  }

  private Set<Set<ColumnIdentifier>> generalizeSet(Set<Set<ColumnIdentifier>> possibleSmallerIND) {
    Set<Set<ColumnIdentifier>> generalizedINDs = new HashSet<>();
    for(Set<ColumnIdentifier> indNode : possibleSmallerIND) {
      Set<Set<ColumnIdentifier>> powerSet = Sets.powerSet(indNode);
      powerSet.removeIf(x -> x.size() != indNode.size() - 1);
      generalizedINDs.addAll(powerSet);
    }
    return generalizedINDs;
  }

  private int cardinalityOfIND(InclusionDependency ind) {
    return ind.getDependant().getColumnIdentifiers().size();
  }

  private boolean isOptimisticBorderFinal(Set<Set<ColumnIdentifier>> optimisticBorder, Set<Set<ColumnIdentifier>> positiveBorder) {
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
        Set<Set<ColumnIdentifier>> newSolution = new HashSet<>();
        newSolution = unpackCartesianProduct(Sets.cartesianProduct(solution,unpackedHead));
        solution = removeSpecializations(newSolution);
      }
    }
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
      refList.add(dependantToReferenced.get(depId));
    }
    ColumnPermutation dep = new ColumnPermutation();
    dep.setColumnIdentifiers(depList);
    ColumnPermutation ref = new ColumnPermutation();
    ref.setColumnIdentifiers(refList);
    return new InclusionDependency(dep, ref);
  }

  private Map<ColumnIdentifier,ColumnIdentifier> convertUnaryINDsToMap(Set<InclusionDependency> unaryINDs) {
    Map<ColumnIdentifier,ColumnIdentifier> uINDs = new HashMap<>();
    for (InclusionDependency ind : unaryINDs) {
      for (int i = 0; i < ind.getDependant().getColumnIdentifiers().size(); i++) {
        uINDs.put(ind.getDependant().getColumnIdentifiers().get(i), ind.getReferenced().getColumnIdentifiers().get(i));
      }
    }
    return uINDs;
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
    dependantToReferenced = convertUnaryINDsToMap(calculateUnaryInclusionDependencies());
    Set<ColumnIdentifier> invertedIND = new HashSet<>(dependantToReferenced.keySet());
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

  private int g3(InclusionDependency toCheck) {
    // TODO implement g3 on database
    return 1;
  }

  // equivalent to d |= i
  private boolean isIND(InclusionDependency ind) {
    return true; // TODO check if IND using other algorithm
  }

  private void initialCandidateCheck(int k) {
    Pair<InclusionDependency, Boolean> checkedIND;
    for(int i = 0; i < k; i++) {
      checkedIND = checkCandidatesForLevel(i);
      if(checkedIND.getValue()) {
        satisfiedINDs.add(checkedIND.getKey());
      } else {
        unsatisfiedINDs.add(checkedIND.getKey());
      }
    }
  }

  private Pair<InclusionDependency, Boolean> checkCandidatesForLevel(int i) {
    // TODO candidate check with other algorithm
    return new Pair<>(new InclusionDependency(null, null), true);
  }

  private Set<InclusionDependency> calculateUnaryInclusionDependencies() {
    // TODO calculate UINDs with other algorithm
    ColumnIdentifier a1 = new ColumnIdentifier("table", "a");
    ColumnIdentifier a2 = new ColumnIdentifier("table", "a2");
    ColumnIdentifier b1 = new ColumnIdentifier("table", "b");
    ColumnIdentifier b2 = new ColumnIdentifier("table", "b2");
    ColumnIdentifier c1 = new ColumnIdentifier("table", "c");
    ColumnIdentifier c2 = new ColumnIdentifier("table", "c2");
    ColumnIdentifier d1 = new ColumnIdentifier("table", "d");
    ColumnIdentifier d2 = new ColumnIdentifier("table", "d2");
    ColumnIdentifier e1 = new ColumnIdentifier("table", "e");
    ColumnIdentifier e2 = new ColumnIdentifier("table", "e2");

    Set<InclusionDependency> unaryINDs = new HashSet<>();
    unaryINDs.add(makeUnaryIND(a1,a2));
    unaryINDs.add(makeUnaryIND(b1,b2));
    unaryINDs.add(makeUnaryIND(c1,c2));
    unaryINDs.add(makeUnaryIND(d1,d2));
    unaryINDs.add(makeUnaryIND(e1,e2));
    return unaryINDs;
  }

  private InclusionDependency makeUnaryIND(ColumnIdentifier dep, ColumnIdentifier ref) {
    return new InclusionDependency(new ColumnPermutation(dep), new ColumnPermutation(ref));
  }
}