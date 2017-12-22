package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.model.RhsPosition;
import de.metanome.algorithms.mind2.model.UindCoordinates;
import de.metanome.algorithms.mind2.utils.CurrentIterator;
import de.metanome.algorithms.mind2.utils.UindCoordinatesReader;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.metanome.algorithms.mind2.utils.IndComparators.RhsComrapator;
import static de.metanome.algorithms.mind2.utils.IndComparators.UindCoordinatesReaderComparator;
import static java.util.stream.Collectors.toList;

public class Mind2 {

    private final CoordinatesRepository repository = new CoordinatesRepository();
    private final Mind2Configuration configuration;

    @Inject
    public Mind2(Mind2Configuration configuration) {
        this.configuration = configuration;
    }

    public void execute() throws AlgorithmExecutionException {
        repository.storeUindCoordinates(configuration);
        Set<Set<InclusionDependency>> maxInds = generateMaxInds();

        collectInds(maxInds);
    }

    private Set<Set<InclusionDependency>> generateMaxInds() throws AlgorithmExecutionException {
        Queue<UindCoordinatesReader> coordinatesQueue = new PriorityQueue<>(new UindCoordinatesReaderComparator());
        for (InclusionDependency uind : configuration.getUnaryInds()) {
            coordinatesQueue.add(repository.getReader(uind));
        }

        Set<Set<InclusionDependency>> maxInds = new HashSet<>(ImmutableSet.of(configuration.getUnaryInds()));
        while (!coordinatesQueue.isEmpty()) {
            Set<UindCoordinates> L = new HashSet<>();
            Set<UindCoordinatesReader> readers = new HashSet<>();
            UindCoordinatesReader reader = coordinatesQueue.remove();
            readers.add(reader);

            UindCoordinates current = reader.current();
            L.add(current);
            while (!coordinatesQueue.isEmpty()) {
                UindCoordinatesReader reader2 = coordinatesQueue.peek();
                UindCoordinates current2 = reader2.current();
                if (!current.getLhsIndex().equals(current2.getLhsIndex())) {
                    break;
                }
                reader = coordinatesQueue.remove();
                readers.add(reader);
                current = reader.current();
                L.add(current);
            }

            Set<Set<InclusionDependency>> subMaxInd = generateSubMaxInds(L, maxInds);
            maxInds = removeSubsets(generateIntersections(maxInds, subMaxInd));

            for (InclusionDependency uind : configuration.getUnaryInds()) {
                if (maxInds.contains(ImmutableSet.of(uind))) {
                    maxInds.remove(ImmutableSet.of(uind));
                }
            }
            if (maxInds.isEmpty()) {
                return new HashSet<>(ImmutableSet.of(configuration.getUnaryInds()));
            }

            Set<InclusionDependency> activeU = new HashSet<>();
            maxInds.forEach(activeU::addAll);
            for (UindCoordinatesReader nextReader : readers) {
                if (nextReader.hasNext() && activeU.contains(nextReader.current().getUind())) {
                    nextReader.next();
                    coordinatesQueue.add(nextReader);
                }
            }
        }
        return maxInds;
    }

    private Set<Set<InclusionDependency>> generateSubMaxInds(Set<UindCoordinates> L, Set<Set<InclusionDependency>> currentMaxInds) {
        Queue<CurrentIterator<RhsPosition>> positionsQueue = new PriorityQueue<>(new RhsComrapator());
        for (UindCoordinates coords : L) {
            ImmutableList<RhsPosition> positions = coords.getRhsIndices().stream()
                    .map(rhsIndex -> new RhsPosition(coords.getUind(), rhsIndex))
                    .collect(toImmutableList());
            positionsQueue.add(new CurrentIterator<>(positions));
        }

        Set<Set<InclusionDependency>> maxInds = new HashSet<>();
        Set<Set<InclusionDependency>> UB = new HashSet<>();
        while (!positionsQueue.isEmpty()) {
            Set<CurrentIterator<RhsPosition>> readers = new HashSet<>();
            CurrentIterator<RhsPosition> reader = positionsQueue.remove();
            readers.add(reader);
            RhsPosition current = reader.current();
            Set<InclusionDependency> Mj = new HashSet<>(ImmutableSet.of(current.getUind()));

            while (!positionsQueue.isEmpty()) {
                CurrentIterator<RhsPosition> reader2 = positionsQueue.peek();
                RhsPosition current2 = reader2.current();
                if (!current.getRhs().equals(current2.getRhs())) {
                    break;
                }
                reader = positionsQueue.remove();
                readers.add(reader);
                current = reader.current();
                Mj.add(current.getUind());
            }

            for (Set<InclusionDependency> M : currentMaxInds) {
                if (Mj.containsAll(M)) {
                    UB.add(M);
                }
            }
            if (UB.equals(currentMaxInds)) {
                maxInds = currentMaxInds;
                break;
            }
            maxInds.add(Mj);
            for (CurrentIterator<RhsPosition> nextReader : readers) {
                if (nextReader.hasNext()) {
                    nextReader.next();
                    positionsQueue.add(nextReader);
                }
            }
        }
        return removeSubsets(maxInds);
    }

    // phi Operator
    private Set<Set<InclusionDependency>> removeSubsets(Set<Set<InclusionDependency>> inds) {
        // TODO(fwindheuser): Clean up
        Set<Set<InclusionDependency>> maxSets = new HashSet<>(inds);
        inds.forEach(ind -> {
            if (inds.stream().anyMatch(ind2 -> !ind.equals(ind2) && ind2.containsAll(ind))) {
                maxSets.remove(ind);
            }
        });
        return maxSets;
    }

    // psi Operator
    private Set<Set<InclusionDependency>> generateIntersections(Set<Set<InclusionDependency>> indsA, Set<Set<InclusionDependency>> indsB) {
        Set<Set<InclusionDependency>> intersections = new HashSet<>();
        Sets.cartesianProduct(ImmutableList.of(indsA, indsB)).forEach(indPair -> {
            Set<InclusionDependency> s1 = indPair.get(0);
            Set<InclusionDependency> s2 = indPair.get(1);
            Set<InclusionDependency> intersection = Sets.intersection(s1, s2);
            if (!intersection.isEmpty()) {
                intersections.add(intersection);
            }
        });
        return intersections;
    }

    private void collectInds(Set<Set<InclusionDependency>> maxInds) throws AlgorithmExecutionException {
        InclusionDependencyResultReceiver resultReceiver = configuration.getResultReceiver();
        for (Set<InclusionDependency> maxInd : maxInds) {
            List<ColumnIdentifier> referencedIds = maxInd.stream()
                    .map(InclusionDependency::getReferenced)
                    .map(ColumnPermutation::getColumnIdentifiers)
                    .flatMap(List::stream)
                    .collect(toList());
            ColumnPermutation referenced = new ColumnPermutation();
            referenced.setColumnIdentifiers(referencedIds);
            List<ColumnIdentifier> dependantIds = maxInd.stream()
                    .map(InclusionDependency::getDependant)
                    .map(ColumnPermutation::getColumnIdentifiers)
                    .flatMap(List::stream)
                    .collect(toList());
            ColumnPermutation dependant = new ColumnPermutation();
            dependant.setColumnIdentifiers(dependantIds);
            InclusionDependency ind = new InclusionDependency(dependant, referenced);
            resultReceiver.receiveResult(ind);
        }
    }
}
