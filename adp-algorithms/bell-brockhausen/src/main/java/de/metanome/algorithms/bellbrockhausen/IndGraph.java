package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;
import de.metanome.algorithms.bellbrockhausen.models.IndTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.Iterables.getOnlyElement;
import static de.metanome.util.Collectors.toImmutableList;
import static de.metanome.util.Collectors.toImmutableSet;
import static java.util.stream.Collectors.toMap;

public class IndGraph {

    private final InclusionDependencyResultReceiver resultReceiver;
    private final DataAccessObject dataAccessObject;
    private final ImmutableList<ColumnIdentifier> candidates;
    private final Map<ColumnIdentifier, Attribute> attributes;
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> fromEdges;
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> toEdges;
    private final Map<ColumnIdentifier, List<IndTest>> tests;

    private int dbTests = 0;

    public IndGraph(
            InclusionDependencyResultReceiver resultReceiver,
            DataAccessObject dataAccessObject,
            ImmutableList<Attribute> attributes) {
        this.resultReceiver = resultReceiver;
        this.dataAccessObject = dataAccessObject;
        this.candidates = attributes.stream().map(Attribute::getColumnIdentifier).collect(toImmutableList());
        this.attributes = attributes.stream().collect(toMap(Attribute::getColumnIdentifier, a -> a));
        fromEdges = candidates.stream().collect(toMap(c -> c, c -> new HashSet<>()));
        toEdges = candidates.stream().collect(toMap(c -> c, c -> new HashSet<>()));
        tests = new HashMap<>();
        buildTests(attributes);
    }

    public void testCandidates() throws AlgorithmExecutionException {
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

    private void buildTests(ImmutableList<Attribute> candidates) {
        for (int i = 0; i < candidates.size(); i++) {
            Attribute candidateA = candidates.get(i);
            for (int j = i + 1; j < candidates.size(); j++) {
                Attribute candidateB = candidates.get(j);
                if (!tests.containsKey(candidateA.getColumnIdentifier())) {
                    tests.put(candidateA.getColumnIdentifier(), new ArrayList<>());
                }
                tests.get(candidateA.getColumnIdentifier()).add(IndTest.fromAttributes(candidateA, candidateB));
                tests.get(candidateA.getColumnIdentifier()).add(IndTest.fromAttributes(candidateB, candidateA));
            }
        }
    }

    private void testCandidate(final InclusionDependency test) throws AlgorithmExecutionException {
        ColumnIdentifier dependant = getDependant(test);
        //ColumnIdentifier referenced = getReferenced(test);
        final int dependantIndex = getCandidateIndex(dependant);
        // Run test if not: has edge A_ref -> A_k with k < i and no edge A_depend -> A_k
        if (fromEdges.get(dependant).stream()
                .noneMatch(node -> getCandidateIndex(node) < dependantIndex && !hasEdge(dependant, node))) {
            if (isInRange(test)) {
                dbTests++;
                if (dataAccessObject.isValidUIND(test)) {
                    updateGraph(test);
                    if (hasEqualRange(test)) {
                        InclusionDependency reversedTest = reverseInd(test);
                        dbTests++;
                        if (dataAccessObject.isValidUIND(reversedTest)) {
                            updateGraph(reversedTest);
                        }
                    }
                }
            }
        }
    }

    private void updateGraph(final InclusionDependency ind)
            throws ColumnNameMismatchException, CouldNotReceiveResultException {
        // A_i -> A_j
        if (hasEdge(getDependant(ind), getReferenced(ind))) {
            return;
        }
        insertEdge(getDependant(ind), getReferenced(ind));
        int dependantIndex = getCandidateIndex(getDependant(ind));
        int referencedIndex = getCandidateIndex(getReferenced(ind));

        if (getCandidateIndex(getDependant(ind)) < getCandidateIndex(getReferenced(ind))) {
            // Find all nodes A_k, k > i with path to node A_i
            ImmutableSet<ColumnIdentifier> toDependant = collectNodesWithPathTo(getDependant(ind), dependantIndex);
            // Find all nodes A_l, l > i reachable from node A_j
            ImmutableSet<ColumnIdentifier> fromReferenced = collectNodesWithPathFrom(getReferenced(ind), dependantIndex);

            // Delete tests A_i -> A_l with l > i in list A_i
            tests.get(getDependant(ind)).forEach(indTest -> {
                if (fromReferenced.contains(getReferenced(indTest.getTest())) &&
                        getCandidateIndex(getReferenced(indTest.getTest())) > getCandidateIndex(getDependant(ind))) {
                    indTest.setDeleted(true);
                }
            });

            deleteTests(toDependant, fromReferenced);
        } else {
            // Find all nodes A_k, k > j with path to node A_i
            ImmutableSet<ColumnIdentifier> toDependant = collectNodesWithPathTo(getDependant(ind), referencedIndex);
            // Find all nodes A_l, l > j reachable from node A_j
            ImmutableSet<ColumnIdentifier> fromReferenced = collectNodesWithPathFrom(getReferenced(ind), referencedIndex);

            // Delete tests A_k -> A_j with k > i in list A_j
            tests.get(getReferenced(ind)).forEach(indTest -> {
                if (toDependant.contains(getDependant(indTest.getTest())) &&
                        getCandidateIndex(getDependant(indTest.getTest())) > dependantIndex) {
                    indTest.setDeleted(true);
                }
            });

            deleteTests(toDependant, fromReferenced);
        }
    }

    private void deleteTests(ImmutableSet<ColumnIdentifier> toDependant, ImmutableSet<ColumnIdentifier> fromReferenced) {
        // Delete tests __A_l__ with k < l in lists A_k
        for (ColumnIdentifier node : toDependant) {
            if (!tests.containsKey(node)) continue;
            int nodeIndex = getCandidateIndex(node);
            tests.get(node).forEach(indTest -> {
                if (fromReferenced.contains(getReferenced(indTest.getTest())) &&
                        nodeIndex < getCandidateIndex(getReferenced(indTest.getTest()))) {
                    indTest.setDeleted(true);
                }
            });
        }

        // Delete tests ''A_k'' with k > l in lists A_l
        for (ColumnIdentifier node : fromReferenced) {
            if (!tests.containsKey(node)) continue;
            int nodeIndex = getCandidateIndex(node);
            tests.get(node).forEach(indTest -> {
                if (toDependant.contains(getDependant(indTest.getTest())) &&
                         getCandidateIndex(getReferenced(indTest.getTest())) > nodeIndex) {
                    indTest.setDeleted(true);
                }
            });
        }
    }

    private ImmutableSet<ColumnIdentifier> collectNodesWithPathTo(ColumnIdentifier node, int nodeIndex) {
        // Find all nodes A_k, k > i with path to node A_i
        return collectNodes(node, nodeIndex, toEdges::get);
    }

    private ImmutableSet<ColumnIdentifier> collectNodesWithPathFrom(ColumnIdentifier node, int nodeIndex) {
        // Find all nodes A_l, l > i reachable from node A_j
        return collectNodes(node, nodeIndex, fromEdges::get);
    }

    private ImmutableSet<ColumnIdentifier> collectNodes(
            ColumnIdentifier node, int nodeIndex,Function<ColumnIdentifier, Set<ColumnIdentifier>> getSuccessor) {
        Set<ColumnIdentifier> results = new HashSet<>();
        Queue<ColumnIdentifier> queue = new LinkedList<>();
        queue.add(node);
        while (!queue.isEmpty()) {
            ColumnIdentifier nextNode = queue.remove();
            ImmutableSet<ColumnIdentifier> nodes = getSuccessor.apply(nextNode).stream()
                    .filter(column -> getCandidateIndex(column) > nodeIndex)
                    .collect(toImmutableSet());
            queue.addAll(Sets.difference(nodes, results));
            results.addAll(nodes);
        }
        return ImmutableSet.copyOf(results);
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

    private int getCandidateIndex(final ColumnIdentifier candidate) {
        // TODO(fwindheuser): Keep index of candidate positions to speed this up
        return candidates.indexOf(candidate);
    }

    private InclusionDependency toInd(final ColumnIdentifier dependant, final ColumnIdentifier referenced) {
        return new InclusionDependency(new ColumnPermutation(dependant), new ColumnPermutation(referenced));
    }

    private ColumnIdentifier getReferenced(final InclusionDependency ind) {
        return getOnlyElement(ind.getReferenced().getColumnIdentifiers());
    }

    private ColumnIdentifier getDependant(final InclusionDependency ind) {
        return getOnlyElement(ind.getDependant().getColumnIdentifiers());
    }

    private InclusionDependency reverseInd(final InclusionDependency ind) {
        return new InclusionDependency(ind.getReferenced(), ind.getDependant());
    }

    private boolean hasEqualRange(final InclusionDependency ind) {
        Attribute dependant = attributes.get(getDependant(ind));
        Attribute referenced = attributes.get(getReferenced(ind));
        return dependant.getValueRange().equals(referenced.getValueRange());
    }

    private boolean isInRange(final InclusionDependency ind) {
        Attribute dependant = attributes.get(getDependant(ind));
        Attribute referenced = attributes.get(getReferenced(ind));
        return referenced.getValueRange().encloses(dependant.getValueRange());
    }

    public int getDBTests() {
        return dbTests;
    }
}
