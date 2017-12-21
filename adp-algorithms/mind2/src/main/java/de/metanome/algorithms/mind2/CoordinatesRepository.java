package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.utils.AttributeIterator;
import de.metanome.algorithms.mind2.utils.AttributeValuePosition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;

public class CoordinatesRepository {

    private Map<InclusionDependency, SetMultimap<Integer, Integer>> repo = new HashMap<>();

    public Set<Integer> getCoordinates(InclusionDependency uind, int i) {
        return repo.get(uind).get(i);
    }

    public void generateCoordinates(
            ImmutableSet<InclusionDependency> unaryInds,
            ImmutableList<RelationalInputGenerator> inputGenerators) throws AlgorithmExecutionException {

        ImmutableMap<ColumnIdentifier, RelationalInputGenerator> attributes = getRelationalInputMap(inputGenerators);

        for (InclusionDependency uind : unaryInds) {
            SetMultimap<Integer, Integer> i2jsMap = generateCoordinates(uind, attributes);
            // TODO(fwindheuser): Save i2jsMap to disk
            repo.put(uind, i2jsMap);
        }
    }

    private SetMultimap<Integer, Integer> generateCoordinates(
            InclusionDependency unaryInd, ImmutableMap<ColumnIdentifier, RelationalInputGenerator> attributes)
            throws AlgorithmExecutionException {
        SetMultimap<Integer, Integer> i2jsMap = MultimapBuilder.hashKeys().hashSetValues().build();

        ColumnIdentifier lhs = getUnaryIdentifier(unaryInd.getDependant());
        ColumnIdentifier rhs = getUnaryIdentifier(unaryInd.getReferenced());
        AttributeIterator curA =  new AttributeIterator(attributes.get(lhs).generateNewCopy(), lhs);
        AttributeIterator curB = new AttributeIterator(attributes.get(rhs).generateNewCopy(), rhs);
        curA.next();
        curB.next();
        while (curA.hasNext() || curB.hasNext()) {
            AttributeValuePosition valA = curA.current();
            AttributeValuePosition valB = curB.current();
            if (valA.getValue().equals(valB.getValue())) {
                Set<Integer> posA = new HashSet<>();
                Set<Integer> posB = new HashSet<>();
                AttributeValuePosition nextValA = curA.current();
                while (nextValA.getValue().equals(valA.getValue())) {
                    posA.add(nextValA.getPosition());
                    if (!curA.hasNext()) break;
                    nextValA = curA.next();
                }
                AttributeValuePosition nextValB = curB.current();
                while (nextValB.getValue().equals(valB.getValue())) {
                    posB.add(nextValB.getPosition());
                    if (!curB.hasNext()) break;
                    nextValB = curB.next();
                }
                if (posA.size() > 0 && posB.size() > 0) {
                    posA.forEach(indexA -> i2jsMap.putAll(indexA, posB));
                }
            } else if (valA.getValue().compareTo(valB.getValue()) < 0) {
                if (!curA.hasNext()) break;
                curA.next();
            } else {
                if (!curB.hasNext()) break;
                curB.next();
            }
        }
        if (curA.current().getValue().equals(curB.current().getValue())) {
            i2jsMap.put(curA.current().getPosition(), curB.current().getPosition());
        }
        curA.close();
        curB.close();
        return i2jsMap;
    }

    private ColumnIdentifier getUnaryIdentifier(ColumnPermutation columnPermutation) {
        return getOnlyElement(columnPermutation.getColumnIdentifiers());
    }

    private ImmutableMap<ColumnIdentifier, RelationalInputGenerator> getRelationalInputMap(
            ImmutableList<RelationalInputGenerator> inputGenerators) throws InputGenerationException {
        Map<ColumnIdentifier, RelationalInputGenerator> relationalInputs = new HashMap<>();
        for (RelationalInputGenerator generator : inputGenerators) {
            try {
                RelationalInput input = generator.generateNewCopy();
                input.columnNames().forEach(columnName -> relationalInputs.put(
                        new ColumnIdentifier(input.relationName(), columnName), generator));
                input.close();
            } catch (Exception e) {
                throw new InputGenerationException(format("Error getting copy of %s", generator), e);
            }
        }
        return ImmutableMap.copyOf(relationalInputs);
    }
}
