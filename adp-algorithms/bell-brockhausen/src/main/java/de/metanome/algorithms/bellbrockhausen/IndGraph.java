package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.models.IndTest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static de.metanome.util.Collectors.toImmutableSet;

public class IndGraph {

    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> fromEdges = new HashMap<>();
    private final Map<ColumnIdentifier, Set<ColumnIdentifier>> toEdges = new HashMap<>();
    private final ImmutableMap<ColumnIdentifier, Integer> attributeIndices;

    public IndGraph(ImmutableMap<ColumnIdentifier, Integer> attributeIndices) {
        this.attributeIndices = attributeIndices;
        attributeIndices.keySet().forEach(e -> {
            fromEdges.put(e, new HashSet<>());
            toEdges.put(e, new HashSet<>());
        });
    }

    // A_l <= A_j, i
    public boolean isValidTest(IndTest test, int testGroupIndex) {
        ColumnIdentifier dependant = getDependant(test.getInd());
        ColumnIdentifier referenced = getReferenced(test.getInd());
        // Invalid if A_j -> A_k with k < i and not A_l -> A_k
        return fromEdges.get(referenced).stream()
                .noneMatch(kNode -> getIndex(kNode) < testGroupIndex && !hasEdge(dependant, kNode));
    }

    public void insertEdge(InclusionDependency ind) {
        fromEdges.get(getDependant(ind)).add(getReferenced(ind));
        toEdges.get(getReferenced(ind)).add(getDependant(ind));
    }

    public ImmutableSet<ColumnIdentifier> collectNodesWithPathTo(ColumnIdentifier node) {
        return collectNodes(node, toEdges::get);
    }

    public ImmutableSet<ColumnIdentifier> collectNodesWithPathFrom(ColumnIdentifier node) {
        return collectNodes(node, fromEdges::get);
    }

    private ImmutableSet<ColumnIdentifier> collectNodes(
            ColumnIdentifier node, Function<ColumnIdentifier, Set<ColumnIdentifier>> getSuccessor) {
        Set<ColumnIdentifier> visited = new HashSet<>();
        Queue<ColumnIdentifier> queue = new LinkedList<>();
        queue.add(node);
        while (!queue.isEmpty()) {
            ColumnIdentifier nextNode = queue.remove();
            if (visited.contains(nextNode)) {
                continue;
            }
            visited.add(nextNode);
            Set<ColumnIdentifier> successor = getSuccessor.apply(nextNode);
            queue.addAll(successor);
        }
        return ImmutableSet.copyOf(visited);
    }

    private boolean hasEdge(ColumnIdentifier from, ColumnIdentifier to) {
        return fromEdges.get(from).contains(to);
    }

    private ColumnIdentifier getReferenced(final InclusionDependency ind) {
        return getOnlyElement(ind.getReferenced().getColumnIdentifiers());
    }

    private ColumnIdentifier getDependant(final InclusionDependency ind) {
        return getOnlyElement(ind.getDependant().getColumnIdentifiers());
    }

    private int getIndex(ColumnIdentifier attribute) {
        return attributeIndices.get(attribute);
    }
}
