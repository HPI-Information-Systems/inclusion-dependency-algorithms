package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.models.IndTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toMap;

public class IndGraph {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final DataAccessObject dataAccessObject;
    private final ImmutableList<ColumnIdentifier> candidates;
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> fromEdges;
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> toEdges;
    private final Map<ColumnIdentifier, List<IndTest>> tests;

    public IndGraph(
            InclusionDependencyResultReceiver resultReceiver,
            DataAccessObject dataAccessObject,
            ImmutableList<ColumnIdentifier> candidates) {
        this.resultReceiver = resultReceiver;
        this.dataAccessObject = dataAccessObject;
        this.candidates = candidates;
        fromEdges = candidates.stream().collect(toMap(c -> c, c -> new HashSet<>()));
        toEdges = candidates.stream().collect(toMap(c -> c, c -> new HashSet<>()));
        tests = new HashMap<>();
    }

    public void testCandidates() throws AlgorithmExecutionException {
        buildTests();

        for (int i = 0; i < candidates.size() - 1; i++) {
            ColumnIdentifier testGroupIndex = candidates.get(i);
            List<IndTest> testGroup = tests.get(testGroupIndex);
            for (IndTest test : testGroup) {
                if (!test.isDeleted()) {
                    testCandidate(test.getTest());
                }
            }
        }
    }

    private void buildTests() {
        for (int i = 0; i < candidates.size(); i++) {
            ColumnIdentifier candidateA = candidates.get(i);
            for (int j = i + 1; j < candidates.size(); j++) {
                ColumnIdentifier candidateB = candidates.get(j);
                if (!tests.containsKey(candidateA)) {
                    tests.put(candidateA, new ArrayList<>());
                }
                tests.get(candidateA).add(new IndTest(toInd(candidateA, candidateB)));
                tests.get(candidateA).add(new IndTest(toInd(candidateB, candidateA)));
            }
        }
    }

    private void testCandidate(final InclusionDependency test) throws AlgorithmExecutionException {
        ColumnIdentifier dependant = getDependant(test);
        ColumnIdentifier referenced = getReferenced(test);
        final int dependantIndex = getCandidateIndex(dependant);
        // Run test if not: has edge A_ref -> A_k with k < i and no edge A_depend -> A_k
        if (fromEdges.get(referenced).stream()
                .noneMatch(node -> getCandidateIndex(node) < dependantIndex && !hasEdge(dependant, node))) {
            if (dataAccessObject.isValidUIND(test)) {
                updateGraph(test);
            }
        }
    }

    private void updateGraph(final InclusionDependency ind)
            throws ColumnNameMismatchException, CouldNotReceiveResultException {
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
            tests.get(getDependant(ind)).forEach(indTest -> {
                if (fromReferenced.contains(getReferenced(indTest.getTest())) &&
                        getCandidateIndex(getReferenced(indTest.getTest())) > getCandidateIndex(getDependant(ind))) {
                    indTest.setDeleted(true);
                }
            });

            // Delete tests A_k -> A_l with k < l in lists A_k
            for (ColumnIdentifier node : toDependant) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).forEach(indTest -> {
                    if (node.equals(getDependant(indTest.getTest())) &&
                            nodeIndex < getCandidateIndex(getReferenced(indTest.getTest()))) {
                        indTest.setDeleted(true);
                    }
                });
            }

            // Delete tests A_l -> A_k with k > l in lists A_l
            for (ColumnIdentifier node: fromReferenced) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).forEach(indTest -> {
                    if (node.equals(getDependant(indTest.getTest())) &&
                            nodeIndex > getCandidateIndex(getReferenced(indTest.getTest()))) {
                        indTest.setDeleted(true);
                    }
                });
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
            tests.get(getReferenced(ind)).forEach(indTest -> {
                if (toDependant.contains(getDependant(indTest.getTest())) &&
                        getCandidateIndex(getDependant(indTest.getTest())) > dependantIndex) {
                    indTest.setDeleted(true);
                }
            });

            // Delete tests A_k -> A_l with k < l in lists A_k
            for (ColumnIdentifier node : toDependant) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).forEach(indTest -> {
                    if (node.equals(getDependant(indTest.getTest())) &&
                            nodeIndex < getCandidateIndex(getReferenced(indTest.getTest()))) {
                        indTest.setDeleted(true);
                    }
                });
            }

            // Delete tests A_k -> A_l with k > l in lists A_l
            for (ColumnIdentifier node : fromReferenced) {
                int nodeIndex = getCandidateIndex(node);
                tests.get(node).forEach(indTest -> {
                    if (node.equals(getDependant(indTest.getTest())) &&
                            nodeIndex > getCandidateIndex(getReferenced(indTest.getTest()))) {
                        indTest.setDeleted(true);
                    }
                });
            }
        }
    }

    private void insertEdge(final ColumnIdentifier dependant, final ColumnIdentifier referenced)
            throws ColumnNameMismatchException, CouldNotReceiveResultException {
        fromEdges.get(dependant).add(referenced);
        toEdges.get(referenced).add(dependant);
        resultReceiver.receiveResult(toInd(dependant, referenced));
    }

    private boolean hasEdge(final ColumnIdentifier dependant, final ColumnIdentifier referenced) {
        return fromEdges.get(dependant).contains(referenced);
    }

    private boolean hasTest(final InclusionDependency testInd) {
        ColumnIdentifier dependant = getDependant(testInd);
        return tests.containsKey(dependant) && tests.get(dependant).contains(testInd);
    }

    private int getCandidateIndex(final ColumnIdentifier candidate) {
        // TODO(fwindheuser): Keep index of candidate positions to speed this up
        return candidates.indexOf(candidate);
    }

    private InclusionDependency toInd(final ColumnIdentifier dependant, final ColumnIdentifier referenced) {
        return new InclusionDependency(new ColumnPermutation(dependant), new ColumnPermutation(referenced));
    }

    private ColumnIdentifier getReferenced(final InclusionDependency ind) {
        return ind.getReferenced().getColumnIdentifiers().get(0);
    }

    private ColumnIdentifier getDependant(final InclusionDependency ind) {
        return ind.getDependant().getColumnIdentifiers().get(0);
    }
}
