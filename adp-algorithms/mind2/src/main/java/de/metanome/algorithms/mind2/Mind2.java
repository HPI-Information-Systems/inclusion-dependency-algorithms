package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.model.UindCoordinates;
import de.metanome.algorithms.mind2.utils.RhsIterator;
import de.metanome.algorithms.mind2.utils.UindCoordinatesReader;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import static de.metanome.algorithms.mind2.utils.IndComparators.RhsComrapator;
import static de.metanome.algorithms.mind2.utils.IndComparators.UindCoordinatesReaderComparator;
import static java.lang.String.format;

public class Mind2 {

    private final static Logger log = Logger.getLogger(Mind2.class.getName());

    private final Mind2Configuration config;

    @Inject
    public Mind2(Mind2Configuration config) {
        this.config = config;
    }

    public void execute(ImmutableSet<InclusionDependency> uinds) throws AlgorithmExecutionException {
        CoordinatesRepository repository = new CoordinatesRepository(config, uinds);
        IntSet uindIds = repository.storeUindCoordinates();
        log.info("Finished calculating UIND coordinates");

        Set<IntSet> maxInds = generateMaxInds(repository, uindIds);
        repository.collectInds(maxInds);
    }

    private Set<IntSet> generateMaxInds(
            CoordinatesRepository repository,
            IntSet uinds) throws AlgorithmExecutionException {
        Queue<UindCoordinatesReader> coordinatesQueue = new PriorityQueue<>(new UindCoordinatesReaderComparator());
        for (int uind : uinds) {
            coordinatesQueue.add(repository.getReader(uind));
        }

        Set<IntSet> maxInds = new ObjectOpenHashSet<>();
        maxInds.add(uinds);

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
                if (currentCoords.getLhsIndex() != nextCoords.getLhsIndex()) {
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
            Set<IntSet> subMaxInd = generateSubMaxInds(sameIndexCoords, maxInds);
            maxInds = removeSubsets(generateIntersections(maxInds, subMaxInd));

            for (int uind : uinds) {
                IntSet uindSet = IntSets.singleton(uind);
                if (maxInds.contains(uindSet)) {
                    maxInds.remove(uindSet);
                }
            }
            if (maxInds.isEmpty()) {
                for (int uind: uinds) {
                    maxInds.add(IntSets.singleton(uind));
                }
                return maxInds;
            }

            IntSet activeU = new IntOpenHashSet();
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

    private Set<IntSet> generateSubMaxInds(
            Set<UindCoordinates> sameIndexCoords, Set<IntSet> currentMaxInds) {

        Queue<RhsIterator> positionsQueue = new PriorityQueue<>(new RhsComrapator());
        initializeQueue(positionsQueue, sameIndexCoords);

        Set<IntSet> maxInds = new ObjectOpenHashSet<>();
        Set<IntSet> maxIndSubsets = new ObjectOpenHashSet<>();
        while (!positionsQueue.isEmpty()) {
            Set<RhsIterator> readers = new HashSet<>();
            RhsIterator reader = positionsQueue.poll();
            readers.add(reader);

            int currentRhsPosition = reader.current();
            int currentRhsUind = reader.getUindId();

            IntSet subMaxInds = new IntOpenHashSet();
            subMaxInds.add(currentRhsUind);

            while (!positionsQueue.isEmpty()) {
                RhsIterator nextReader = positionsQueue.peek();
                int nextRhsPosition = nextReader.current();
                if (currentRhsPosition != nextRhsPosition) {
                    break;
                }
                reader = positionsQueue.poll();
                readers.add(reader);
                currentRhsPosition = reader.current();
                currentRhsUind = reader.getUindId();
                subMaxInds.add(currentRhsUind);
            }

            subMaxIndsTerminationCheck(currentMaxInds, maxIndSubsets, subMaxInds);
            if (maxIndSubsets.equals(currentMaxInds)) {
                maxInds = currentMaxInds;
                break;
            }
            mergeSubMaxInds(maxInds, subMaxInds);
            advanceReader(positionsQueue, readers);
        }
        return removeSubsets(maxInds);
    }

    private void initializeQueue(
            Queue<RhsIterator> positionsQueue,
            Set<UindCoordinates> sameIndexCoords) {
        for (UindCoordinates coords : sameIndexCoords) {
            positionsQueue.add(new RhsIterator(coords.getUindId(), coords.getRhsIndices()));
        }
    }

    private void subMaxIndsTerminationCheck(
            Set<IntSet> currentMaxInds,
            Set<IntSet> maxIndSubsets,
            IntSet subMaxInds) {
        for (IntSet maxInd : currentMaxInds) {
            if (subMaxInds.containsAll(maxInd)) {
                maxIndSubsets.add(maxInd);
            }
        }
    }

    private void mergeSubMaxInds(Set<IntSet> maxInds, IntSet subMaxInds) {
        maxInds.add(subMaxInds);
    }

    private void advanceReader(
            Queue<RhsIterator> positionsQueue,
            Set<RhsIterator> readers) {
        for (RhsIterator nextReader : readers) {
            if (nextReader.hasNext()) {
                nextReader.next();
                positionsQueue.add(nextReader);
            }
        }
    }

    // phi Operator
    private Set<IntSet> removeSubsets(Set<IntSet> inds) {
        ImmutableList<IntSet> orderedInds = ImmutableList.copyOf(inds);
        Set<IntSet> maxSets = new ObjectOpenHashSet<>(inds);
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
    private Set<IntSet> generateIntersections(
            Set<IntSet> indsA, Set<IntSet> indsB) {
        ObjectSet<IntSet> intersections = new ObjectOpenHashSet<>();
        Sets.cartesianProduct(ImmutableList.of(indsA, indsB)).forEach(indPair -> {
            ImmutableSet<Integer> intersection = Sets.intersection(indPair.get(0), indPair.get(1))
                    .immutableCopy();
            if (!intersection.isEmpty()) {
                intersections.add(new IntOpenHashSet(intersection));
            }
        });
        return intersections;
    }
}
