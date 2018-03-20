package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;
import de.metanome.algorithms.bellbrockhausen.models.IndTest;
import de.metanome.algorithms.bellbrockhausen.models.IndTestPair;

import java.util.logging.Logger;

import static de.metanome.util.Collectors.toImmutableSet;
import static de.metanome.util.UindUtils.getDependant;
import static de.metanome.util.UindUtils.getReferenced;
import static java.lang.String.format;

public class IndCandidates {

    public final static Logger log = Logger.getLogger(IndCandidates.class.getName());

    private final InclusionDependencyResultReceiver resultReceiver;
    private final DataAccessObject dataAccessObject;

    private final ImmutableList<ImmutableList<IndTestPair>> tests;
    private final ImmutableMap<ColumnIdentifier, Integer> attributeIndices;
    private final IndGraph indGraph;

    private int dbTests = 0;
    private int validUinds = 0;

    public IndCandidates(
            InclusionDependencyResultReceiver resultReceiver,
            DataAccessObject dataAccessObject,
            ImmutableList<Attribute> attributes) {
        this.resultReceiver = resultReceiver;
        this.dataAccessObject = dataAccessObject;

        this.attributeIndices = getAttributeIndices(attributes);
        this.tests = getTests(attributes);
        this.indGraph = new IndGraph(attributeIndices);
    }

    public void testCandidates() throws AlgorithmExecutionException {
        for (int i = 0; i < tests.size(); i++) {
            ImmutableList<IndTestPair> testPairs = tests.get(i);
            log.info(format("=== Testing candidate group %d of %d ===", i, tests.size()));
            printStats();
            for (IndTestPair testPair : testPairs) {
                testCandidate(testPair.getFromBase(), i);
                testCandidate(testPair.getToBase(), i);
            }
        }
    }

    private ImmutableMap<ColumnIdentifier, Integer> getAttributeIndices(ImmutableList<Attribute> candidates) {
        ImmutableMap.Builder<ColumnIdentifier, Integer> indices = ImmutableMap.builder();
        for (int i = 0; i < candidates.size(); i++) {
           indices.put(candidates.get(i).getColumnIdentifier(), i);
        }
        return indices.build();
    }

    private ImmutableList<ImmutableList<IndTestPair>> getTests(ImmutableList<Attribute> candidates) {
        ImmutableList.Builder<ImmutableList<IndTestPair>> tests = ImmutableList.builder();
        for (int i = 0; i < candidates.size(); i++) {
            ImmutableList.Builder<IndTestPair> testPairs = ImmutableList.builder();
            Attribute baseCandidate = candidates.get(i);
            for (int j = i + 1; j < candidates.size(); j++) {
                Attribute iterateCandidate = candidates.get(j);
                testPairs.add(IndTestPair.fromAttributes(baseCandidate, iterateCandidate));
            }
            tests.add(testPairs.build());
        }
        return tests.build();
    }

    private void testCandidate(IndTest test, int testGroupIndex) throws AlgorithmExecutionException {
        if (test.isDeleted() || indGraph.isValidTest(test, testGroupIndex) && isValidInd(test)) {
            updateGraph(test.getInd());
            resultReceiver.receiveResult(test.getInd());
            validUinds++;
        }
    }

    private boolean isValidInd(IndTest test) throws AlgorithmExecutionException {
        if (!test.getReferenced().getValueRange().encloses(test.getDependant().getValueRange())) {
            return false;
        }
        dbTests++;
        return dataAccessObject.isValidUIND(test.getInd());
    }

    // A_i -> A_j
    private void updateGraph(InclusionDependency ind) {
        indGraph.insertEdge(ind);
        ColumnIdentifier dependant = getDependant(ind);
        ColumnIdentifier referenced = getReferenced(ind);
        int dependentIndex = getIndex(dependant);
        int referencedIndex = getIndex(referenced);

        if (dependentIndex < referencedIndex) {
            ImmutableSet<ColumnIdentifier> toDependant = indGraph.collectNodesWithPathTo(dependant).stream()
                    .filter(kNode -> getIndex(kNode) > dependentIndex)
                    .collect(toImmutableSet());
            ImmutableSet<ColumnIdentifier> fromReferenced = indGraph.collectNodesWithPathFrom(referenced).stream()
                    .filter(lNode -> getIndex(lNode) > dependentIndex)
                    .collect(toImmutableSet());

            // Delete A_i <= A_l with l > j in A_i[]
            tests.get(dependentIndex).forEach(testPair -> {
                IndTest test = testPair.getFromBase();
                ColumnIdentifier testReferenced = test.getReferenced().getColumnIdentifier();
                if (fromReferenced.contains(testReferenced) && getIndex(testReferenced) > referencedIndex) {
                    test.delete();
                }
            });
            deleteTests(toDependant, fromReferenced);
        } else {
            ImmutableSet<ColumnIdentifier> toDependant = indGraph.collectNodesWithPathTo(dependant).stream()
                    .filter(kNode -> getIndex(kNode) > referencedIndex)
                    .collect(toImmutableSet());
            ImmutableSet<ColumnIdentifier> fromReferenced = indGraph.collectNodesWithPathFrom(referenced).stream()
                    .filter(lNode -> getIndex(lNode) > referencedIndex)
                    .collect(toImmutableSet());

            // Delete A_k <= A_j with k > i in A_j[]
            tests.get(referencedIndex).forEach(testPair -> {
                IndTest test = testPair.getToBase();
                ColumnIdentifier testDependant = test.getDependant().getColumnIdentifier();
                if (toDependant.contains(testDependant) && getIndex(testDependant) > dependentIndex) {
                    test.delete();
                }
            });
            deleteTests(toDependant, fromReferenced);
        }
    }

    // A_k[], A_l[]
    private void deleteTests(ImmutableSet<ColumnIdentifier> toDependant, ImmutableSet<ColumnIdentifier> fromReferenced) {
        // Delete all A_k <= A_l with k < l in A_k[]
        toDependant.forEach(kAttribute -> {
            tests.get(getIndex(kAttribute)).forEach(testPair -> {
                IndTest test = testPair.getFromBase();
                ColumnIdentifier testDependant = test.getDependant().getColumnIdentifier();
                ColumnIdentifier testReferenced = test.getReferenced().getColumnIdentifier();
                if (fromReferenced.contains(testReferenced) && getIndex(testDependant) < getIndex(testReferenced)) {
                    test.delete();
                }
            });
        });

        // Delete all A_k <= A_l with k > l in A_l[]
        fromReferenced.forEach(lAttribute -> {
            tests.get(getIndex(lAttribute)).forEach(testPair -> {
                IndTest test = testPair.getToBase();
                ColumnIdentifier testDependant = test.getDependant().getColumnIdentifier();
                ColumnIdentifier testReferenced = test.getReferenced().getColumnIdentifier();
                if (toDependant.contains(testDependant) && getIndex(testDependant) > getIndex(testReferenced)) {
                    test.delete();
                }
            });
        });
    }

    private int getIndex(ColumnIdentifier attribute) {
        return attributeIndices.get(attribute);
    }

    public int getDBTests() {
        return dbTests;
    }

    public int getNumberOfDeletedTests() {
        int deletedTests = 0;
        for (ImmutableList<IndTestPair> testGroups : tests) {
            for (IndTestPair testPair: testGroups) {
                if (testPair.getFromBase().isDeleted()) deletedTests++;
                if (testPair.getToBase().isDeleted()) deletedTests++;
            }
        }
        return deletedTests;
    }

    public void printStats() {
        log.info(format("#DBTests: %d - #DeletedTests: %d - #UINDs: %d", dbTests, getNumberOfDeletedTests(), validUinds));
    }
}

