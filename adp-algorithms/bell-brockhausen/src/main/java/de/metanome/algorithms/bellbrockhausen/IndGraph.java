package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class IndGraph {

    private final ImmutableList<ColumnIdentifier> candidates;
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> fromEdges;
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> toEdges;
    private final Map<ColumnIdentifier, List<InclusionDependency>> tests;

    public IndGraph(ImmutableList<ColumnIdentifier> candidates) {
        this.candidates = candidates;
        fromEdges = new HashMap<>();
        toEdges = new HashMap<>();
        tests = new HashMap<>();
        buildTests();

    }

    private Map<ColumnIdentifier, List<InclusionDependency>> buildTests() {
        for (int i = 0; i < candidates.size(); i++) {
            ColumnIdentifier candidateA = candidates.get(i);
            for (int j = i + 1; j < candidates.size(); j++) {
                ColumnIdentifier candidateB = candidates.get(j);
                if (!tests.containsKey(candidateA)) {
                    tests.put(candidateA, new ArrayList<>());
                }
                tests.get(candidateA).add(toInd(candidateA, candidateB));
                tests.get(candidateA).add(toInd(candidateB, candidateA));
            }
        }
        return tests;
    }

    public void updateGraph(final InclusionDependency ind) {
        // A_i -> A_j
        insertEdge(getDependant(ind), getReferenced(ind));
        int dependantIndex = getCandidateIndex(getDependant(ind));
        int referencedIndex = getCandidateIndex(getReferenced(ind));
        if (getCandidateIndex(getDependant(ind)) < getCandidateIndex(getReferenced(ind))) {
            // Find all nodes A_k, k > i with path to node A_i
            ImmutableList<ColumnIdentifier> toDependant = toEdges.get(getDependant(ind)).stream()
                    .filter(column -> getCandidateIndex(column) > dependantIndex)
                    .collect(toImmutableList());
            // Find all nodes A_l, l > i reachable from node A_j
            ImmutableList<ColumnIdentifier> fromReferenced = fromEdges.get(getReferenced(ind)).stream()
                    .filter(column -> getCandidateIndex(column) > dependantIndex)
                    .collect(toImmutableList());

            // Delete tests A_i -> A_l with l > i in list A_i
            tests.get(getDependant(ind)).removeIf(indTest -> getDependant(ind).equals(getDependant(indTest)) &&
                        getCandidateIndex(getReferenced(indTest)) > getCandidateIndex(getReferenced(ind)));

            // Delete tests A_k -> A_l with k < l in lists A_k
            for (ColumnIdentifier node : toDependant) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).removeIf(indTest -> node.equals(getDependant(indTest)) &&
                        nodeIndex < getCandidateIndex(getReferenced(indTest)));
            }

            // Delete tests A_l -> A_k with k > l in lists A_l
            for (ColumnIdentifier node: fromReferenced) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).removeIf(indTest -> node.equals(getDependant(indTest)) &&
                        nodeIndex > getCandidateIndex(getReferenced(indTest)));
            }
        } else {
            // Find all nodes A_k, k > j with path to node A_i
            ImmutableList<ColumnIdentifier> toDependant = toEdges.get(getDependant(ind)).stream()
                    .filter(column -> getCandidateIndex(column) > referencedIndex)
                    .collect(toImmutableList());
            // Find all nodes A_l, l > j reachable from node A_j
            ImmutableList<ColumnIdentifier> fromReferenced = fromEdges.get(getReferenced(ind)).stream()
                    .filter(column -> getCandidateIndex(column) > referencedIndex)
                    .collect(toImmutableList());

            // Delete tests A_k -> A_j with k > i in list A_j
            tests.get(getReferenced(ind)).removeIf(indTest -> getReferenced(ind).equals(getReferenced(indTest)) &&
                    getCandidateIndex(getDependant(indTest)) > dependantIndex);

            // Delete tests A_k -> A_l with k < l in lists A_k
            for (ColumnIdentifier node : toDependant) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).removeIf(indTest -> node.equals(getDependant(indTest)) &&
                        nodeIndex < getCandidateIndex(getReferenced(indTest)));
            }

            // Delete tests A_k -> A_l with k > l in lists A_l
            for (ColumnIdentifier node : fromReferenced) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).removeIf(indTest -> node.equals(getDependant(indTest)) &&
                        nodeIndex > getCandidateIndex(getReferenced(indTest)));
            }
        }
    }

    private InclusionDependency toInd(final ColumnIdentifier referenced, final ColumnIdentifier dependant) {
        return new InclusionDependency(new ColumnPermutation(referenced), new ColumnPermutation(dependant));
    }

    private ColumnIdentifier getReferenced(final InclusionDependency ind) {
        return ind.getReferenced().getColumnIdentifiers().get(0);
    }

    private ColumnIdentifier getDependant(final InclusionDependency ind) {
        return ind.getDependant().getColumnIdentifiers().get(0);
    }

    private void insertEdge(final ColumnIdentifier dependant, final ColumnIdentifier referenced) {
        fromEdges.get(dependant).add(referenced);
        toEdges.get(referenced).add(dependant);
    }

    private int getCandidateIndex(final ColumnIdentifier candidate) {
        return candidates.indexOf(candidate);
    }
}
