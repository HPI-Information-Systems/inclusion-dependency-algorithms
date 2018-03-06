package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
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

import static de.metanome.algorithms.mind2.utils.IndComparators.RhsComrapator;
import static de.metanome.algorithms.mind2.utils.IndComparators.UindCoordinatesReaderComparator;
import static de.metanome.util.Collectors.toImmutableSet;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class Mind2 {

    private final static Logger log = Logger.getLogger(Mind2.class.getName());

    private final Mind2Configuration config;

    @Inject
    public Mind2(Mind2Configuration config) {
        this.config = config;
    }

    public void execute(ImmutableSet<InclusionDependency> uinds) throws AlgorithmExecutionException {
        CoordinatesRepository repository = new CoordinatesRepository(config, uinds);
        ImmutableSet<Integer> uindIds = repository.storeUindCoordinates();
        log.info("Finished calculating UIND coordinates");

        Set<Set<Integer>> maxInds = generateMaxInds(repository, uindIds);
        repository.collectInds(maxInds);
    }

    private Set<Set<Integer>> generateMaxInds(
            CoordinatesRepository repository,
            ImmutableSet<Integer> uinds) throws AlgorithmExecutionException {
        Queue<UindCoordinatesReader> coordinatesQueue = new PriorityQueue<>(new UindCoordinatesReaderComparator());
        for (int uind : uinds) {
            coordinatesQueue.add(repository.getReader(uind));
        }

        Set<Set<Integer>> maxInds = ImmutableSet.of(uinds);

        while (!coordinatesQueue.isEmpty()) {
            Set<UindCoordinates> sameIndexCoords = new HashSet<>();
            Set<UindCoordinatesReader> readers = new HashSet<>();
            UindCoordinatesReader reader = coordinatesQueue.poll();
            readers.add(reader);

            UindCoordinates currentCoords = reader.current();
            sameIndexCoords.add(currentCoords);
            while (!coordinatesQueue.isEmpty()) {
                UindCoordinatesReader nextReader = coordinatesQueue.peek();
                UindCoordinates nextCoords = nextReader.current();
                if (!currentCoords.getLhsIndex().equals(nextCoords.getLhsIndex())) {
                    break;
                }
                reader = coordinatesQueue.poll();
                readers.add(reader);
                currentCoords = reader.current();
                sameIndexCoords.add(currentCoords);
            }

            if (currentCoords.getLhsIndex() % 5000 == 0) {
                log.info(format("Calculate sub max inds for index %d", currentCoords.getLhsIndex()));
            }
            Set<Set<Integer>> subMaxInd = generateSubMaxInds(sameIndexCoords, maxInds);
            maxInds = removeSubsets(generateIntersections(maxInds, subMaxInd));

            for (int uind : uinds) {
                ImmutableSet<Integer> uindSet = ImmutableSet.of(uind);
                if (maxInds.contains(uindSet)) {
                    maxInds.remove(uindSet);
                }
            }
            if (maxInds.isEmpty()) {
                return uinds.stream().map(ImmutableSet::of).collect(toImmutableSet());
            }

            Set<Integer> activeU = new HashSet<>();
            maxInds.forEach(activeU::addAll);
            for (UindCoordinatesReader nextReader : readers) {
                if (nextReader.hasNext() && activeU.contains(nextReader.current().getUindId())) {
                    nextReader.next();
                    coordinatesQueue.add(nextReader);
                } else if (!nextReader.hasNext()) {
                    nextReader.close();
                }
            }
        }
        return maxInds;
    }

    private Set<Set<Integer>> generateSubMaxInds(
            Set<UindCoordinates> sameIndexCoords, Set<Set<Integer>> currentMaxInds) {
        Queue<CurrentIterator<RhsPosition>> positionsQueue = new PriorityQueue<>(new RhsComrapator());
        for (UindCoordinates coords : sameIndexCoords) {
            List<RhsPosition> positions = coords.getRhsIndices().stream()
                    .map(rhsIndex -> new RhsPosition(coords.getUindId(), rhsIndex))
                    .collect(toList());
            positionsQueue.add(new CurrentIterator<>(positions));
        }

        Set<Set<Integer>> maxInds = new HashSet<>();
        Set<Set<Integer>> maxIndSubsets = new HashSet<>();
        while (!positionsQueue.isEmpty()) {
            Set<CurrentIterator<RhsPosition>> readers = new HashSet<>();
            CurrentIterator<RhsPosition> reader = positionsQueue.poll();
            readers.add(reader);
            RhsPosition currentRhsPosition = reader.current();
            Set<Integer> subMaxInds = new HashSet<>(ImmutableSet.of(currentRhsPosition.getUindId()));

            while (!positionsQueue.isEmpty()) {
                CurrentIterator<RhsPosition> nextReader = positionsQueue.peek();
                RhsPosition nextRhsPosition = nextReader.current();
                if (!currentRhsPosition.getRhs().equals(nextRhsPosition.getRhs())) {
                    break;
                }
                reader = positionsQueue.poll();
                readers.add(reader);
                currentRhsPosition = reader.current();
                subMaxInds.add(currentRhsPosition.getUindId());
            }

            for (Set<Integer> maxInd : currentMaxInds) {
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
    private Set<Set<Integer>> removeSubsets(Set<Set<Integer>> inds) {
        ImmutableList<Set<Integer>> orderedInds = ImmutableList.copyOf(inds);
        Set<Set<Integer>> maxSets = new HashSet<>(inds);
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
    private Set<Set<Integer>> generateIntersections(
            Set<Set<Integer>> indsA, Set<Set<Integer>> indsB) {
        Set<Set<Integer>> intersections = new HashSet<>();
        Sets.cartesianProduct(ImmutableList.of(indsA, indsB)).forEach(indPair -> {
            ImmutableSet<Integer> intersection = Sets.intersection(indPair.get(0), indPair.get(1))
                    .immutableCopy();
            if (!intersection.isEmpty()) {
                intersections.add(intersection);
            }
        });
        return intersections;
    }
}
