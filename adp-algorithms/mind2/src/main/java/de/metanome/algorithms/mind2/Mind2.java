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
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static de.metanome.algorithms.mind2.utils.IndComparators.RhsComrapator;
import static de.metanome.algorithms.mind2.utils.IndComparators.UindCoordinatesReaderComparator;
import static de.metanome.util.Collectors.toImmutableList;
import static de.metanome.util.Collectors.toImmutableSet;
import static java.lang.String.format;

public class Mind2 {

    private final static Logger log = Logger.getLogger(Mind2.class.getName());

    private final Mind2Configuration config;
    private CoordinatesRepository repository;

    @Inject
    public Mind2(Mind2Configuration config) {
        this.config = config;
    }

    public void execute() throws AlgorithmExecutionException {
        log.info(format("Start calculating max INDs for %d UINDs", config.getUnaryInds().size()));
        repository = new CoordinatesRepository(config);
        repository.storeUindCoordinates();
        log.info("Finished calculating UIND coordinates");

        Set<Set<InclusionDependency>> maxInds = generateMaxInds();
        collectInds(maxInds);
    }

    private Set<Set<InclusionDependency>> generateMaxInds() throws AlgorithmExecutionException {
        Queue<UindCoordinatesReader> coordinatesQueue = new PriorityQueue<>(new UindCoordinatesReaderComparator());
        for (InclusionDependency uind : config.getUnaryInds()) {
            coordinatesQueue.add(repository.getReader(uind));
        }
        Set<Set<InclusionDependency>> maxInds = config.getUnaryInds().stream()
                .map(ImmutableSet::of)
                .collect(Collectors.toSet());

        while (!coordinatesQueue.isEmpty()) {
            Set<UindCoordinates> sameIndexCoords = new HashSet<>();
            Set<UindCoordinatesReader> readers = new HashSet<>();
            UindCoordinatesReader reader = coordinatesQueue.remove();
            readers.add(reader);

            UindCoordinates currentCoords = reader.current();
            sameIndexCoords.add(currentCoords);
            while (!coordinatesQueue.isEmpty()) {
                UindCoordinatesReader nextReader = coordinatesQueue.peek();
                UindCoordinates nextCoords = nextReader.current();
                if (!currentCoords.getLhsIndex().equals(nextCoords.getLhsIndex())) {
                    break;
                }
                reader = coordinatesQueue.remove();
                readers.add(reader);
                currentCoords = reader.current();
                sameIndexCoords.add(currentCoords);
            }

            Set<Set<InclusionDependency>> subMaxInd = generateSubMaxInds(sameIndexCoords, maxInds);
            maxInds = removeSubsets(generateIntersections(maxInds, subMaxInd));

            for (InclusionDependency uind : config.getUnaryInds()) {
                ImmutableSet<InclusionDependency> uindSet = ImmutableSet.of(uind);
                if (maxInds.contains(uindSet)) {
                    maxInds.remove(uindSet);
                }
            }
            if (maxInds.isEmpty()) {
                return config.getUnaryInds().stream().map(ImmutableSet::of).collect(toImmutableSet());
            }

            Set<InclusionDependency> activeU = new HashSet<>();
            maxInds.forEach(activeU::addAll);
            for (UindCoordinatesReader nextReader : readers) {
                if (nextReader.hasNext() && activeU.contains(nextReader.current().getUind())) {
                    nextReader.next();
                    coordinatesQueue.add(nextReader);
                } else if (!nextReader.hasNext()) {
                    nextReader.close();
                }
            }
        }
        return maxInds;
    }

    private Set<Set<InclusionDependency>> generateSubMaxInds(
            Set<UindCoordinates> sameIndexCoords, Set<Set<InclusionDependency>> currentMaxInds) {
        Queue<CurrentIterator<RhsPosition>> positionsQueue = new PriorityQueue<>(new RhsComrapator());
        for (UindCoordinates coords : sameIndexCoords) {
            ImmutableList<RhsPosition> positions = coords.getRhsIndices().stream()
                    .map(rhsIndex -> new RhsPosition(coords.getUind(), rhsIndex))
                    .collect(toImmutableList());
            positionsQueue.add(new CurrentIterator<>(positions));
        }

        Set<Set<InclusionDependency>> maxInds = new HashSet<>();
        Set<Set<InclusionDependency>> maxIndSubsets = new HashSet<>();
        while (!positionsQueue.isEmpty()) {
            Set<CurrentIterator<RhsPosition>> readers = new HashSet<>();
            CurrentIterator<RhsPosition> reader = positionsQueue.remove();
            readers.add(reader);
            RhsPosition currentRhsPosition = reader.current();
            Set<InclusionDependency> subMaxInds = new HashSet<>(ImmutableSet.of(currentRhsPosition.getUind()));

            while (!positionsQueue.isEmpty()) {
                CurrentIterator<RhsPosition> nextReader = positionsQueue.peek();
                RhsPosition nextRhsPosition = nextReader.current();
                if (!currentRhsPosition.getRhs().equals(nextRhsPosition.getRhs())) {
                    break;
                }
                reader = positionsQueue.remove();
                readers.add(reader);
                currentRhsPosition = reader.current();
                subMaxInds.add(currentRhsPosition.getUind());
            }

            for (Set<InclusionDependency> maxInd : currentMaxInds) {
                if (subMaxInds.containsAll(maxInd)) {
                    maxIndSubsets.add(maxInd);
                }
            }
            if (maxIndSubsets.equals(currentMaxInds)) {
                maxInds = currentMaxInds;
                break;
            }
            maxInds.add(subMaxInds);
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
        ImmutableList<Set<InclusionDependency>> orderedInds = ImmutableList.copyOf(inds);
        Set<Set<InclusionDependency>> maxSets = new HashSet<>(inds);
        for (int i = 0; i < orderedInds.size(); i++) {
            for (int j = 0; j < orderedInds.size(); j++) {
                if (i != j && orderedInds.get(j).containsAll(orderedInds.get(i))) {
                    maxSets.remove(orderedInds.get(i));
                    break;
                }
            }
        }
        return maxSets;
    }

    // psi Operator
    private Set<Set<InclusionDependency>> generateIntersections(
            Set<Set<InclusionDependency>> indsA, Set<Set<InclusionDependency>> indsB) {
        Set<Set<InclusionDependency>> intersections = new HashSet<>();
        Sets.cartesianProduct(ImmutableList.of(indsA, indsB)).forEach(indPair -> {
            ImmutableSet<InclusionDependency> intersection = Sets.intersection(indPair.get(0), indPair.get(1))
                    .immutableCopy();
            if (!intersection.isEmpty()) {
                intersections.add(intersection);
            }
        });
        return intersections;
    }

    private void collectInds(Set<Set<InclusionDependency>> maxInds) throws AlgorithmExecutionException {
        InclusionDependencyResultReceiver resultReceiver = config.getResultReceiver();
        for (Set<InclusionDependency> maxInd : maxInds) {
            ImmutableList<ColumnIdentifier> referencedIds = maxInd.stream()
                    .map(InclusionDependency::getReferenced)
                    .map(ColumnPermutation::getColumnIdentifiers)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
            ColumnPermutation referenced = new ColumnPermutation();
            referenced.setColumnIdentifiers(referencedIds);
            ImmutableList<ColumnIdentifier> dependantIds = maxInd.stream()
                    .map(InclusionDependency::getDependant)
                    .map(ColumnPermutation::getColumnIdentifiers)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
            ColumnPermutation dependant = new ColumnPermutation();
            dependant.setColumnIdentifiers(dependantIds);
            resultReceiver.receiveResult(new InclusionDependency(dependant, referenced));
        }
    }
}
