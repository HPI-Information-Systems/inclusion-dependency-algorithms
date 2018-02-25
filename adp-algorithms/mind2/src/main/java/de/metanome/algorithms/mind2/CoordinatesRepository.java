package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.algorithm_execution.FileCreationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.model.AttributeValuePosition;
import de.metanome.algorithms.mind2.model.UindCoordinates;
import de.metanome.algorithms.mind2.utils.AttributeIterator;
import de.metanome.algorithms.mind2.utils.UindCoordinatesReader;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static de.metanome.util.Collectors.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CoordinatesRepository {

    private final Mind2Configuration config;
    private final ImmutableSet<InclusionDependency> uinds;
    private final Map<InclusionDependency, Path> uindToPath = new HashMap<>();

    @Inject
    public CoordinatesRepository(Mind2Configuration config, ImmutableSet<InclusionDependency> uinds) {
        this.config = config;
        this.uinds = uinds;
    }

    public UindCoordinatesReader getReader(InclusionDependency uind) throws AlgorithmExecutionException {
        if (!uindToPath.containsKey(uind)) {
            throw new AlgorithmExecutionException(format("No coordinates file found for uind %s", uind));
        }
        try {
            return new UindCoordinatesReader(uind, Files.newBufferedReader(uindToPath.get(uind)));
        } catch (IOException e) {
            throw new AlgorithmExecutionException(format("Error reading coordinates file for uind %s", uind), e);
        }
    }

    public void storeUindCoordinates() throws AlgorithmExecutionException {
        ImmutableMap<ColumnIdentifier, TableInputGenerator> attributes =
                getRelationalInputMap(config.getInputGenerators());
        ImmutableMap<String, ColumnIdentifier> indexColumns = getIndexColumns(attributes);
        for (InclusionDependency uind : uinds) {
            SetMultimap<Integer, Integer> uindCoordinates = generateCoordinates(uind, attributes, indexColumns);
            Path path = getPath();
            try {
                writeToFile(uind, uindCoordinates, path);
                uindToPath.put(uind, path);
            } catch (IOException e) {
                throw new AlgorithmExecutionException(
                        format("Error writing uind coordinates for uind %s to file %s", uind, path), e);
            }
        }
    }

    private SetMultimap<Integer, Integer> generateCoordinates(
            InclusionDependency unaryInd,
            ImmutableMap<ColumnIdentifier, TableInputGenerator> attributes,
            ImmutableMap<String, ColumnIdentifier> indexColumns) throws AlgorithmExecutionException {
        SetMultimap<Integer, Integer> uindCoordinates = MultimapBuilder.hashKeys().hashSetValues().build();
        ColumnIdentifier lhs = getUnaryIdentifier(unaryInd.getDependant());
        ColumnIdentifier rhs = getUnaryIdentifier(unaryInd.getReferenced());

        RelationalInput inputA = config.getDataAccessObject()
                .getSortedRelationalInput(attributes.get(lhs), lhs, getIndexColumn(indexColumns, lhs), config.getIndexColumn(), false);
        RelationalInput inputB = config.getDataAccessObject().
                getSortedRelationalInput(attributes.get(rhs), rhs, getIndexColumn(indexColumns, rhs), config.getIndexColumn(), false);
        AttributeIterator cursorA =  new AttributeIterator(inputA, lhs, config.getIndexColumn());
        AttributeIterator cursorB = new AttributeIterator(inputB, rhs, config.getIndexColumn());

        cursorA.next();
        cursorB.next();

        while (cursorA.hasNext() || cursorB.hasNext()) {
            AttributeValuePosition valA = cursorA.current();
            AttributeValuePosition valB = cursorB.current();

            if (valA.getValue().equals(valB.getValue())) {
                Set<Integer> positionsA = new HashSet<>();
                Set<Integer> positionsB = new HashSet<>();

                AttributeValuePosition nextValA = cursorA.current();
                while (nextValA.getValue().equals(valA.getValue())) {
                    positionsA.add(nextValA.getPosition());
                    if (!cursorA.hasNext()) {
                        break;
                    }
                    nextValA = cursorA.next();
                }

                AttributeValuePosition nextValB = cursorB.current();
                while (nextValB.getValue().equals(valB.getValue())) {
                    positionsB.add(nextValB.getPosition());
                    if (!cursorB.hasNext()) {
                        break;
                    }
                    nextValB = cursorB.next();
                }

                if (positionsA.size() > 0 && positionsB.size() > 0) {
                    positionsA.forEach(indexA -> uindCoordinates.putAll(indexA, positionsB));
                }
            } else if (valA.getValue().compareTo(valB.getValue()) < 0) {
                if (!cursorA.hasNext()) {
                    break;
                }
                cursorA.next();
            } else {
                if (!cursorB.hasNext()) {
                    break;
                }
                cursorB.next();
            }
        }
        if (cursorA.current().getValue().equals(cursorB.current().getValue())) {
            uindCoordinates.put(cursorA.current().getPosition(), cursorB.current().getPosition());
        }
        cursorA.close();
        cursorB.close();
        return uindCoordinates;
    }

    private ImmutableMap<ColumnIdentifier, TableInputGenerator> getRelationalInputMap(
            ImmutableList<TableInputGenerator> inputGenerators) throws InputGenerationException {
        Map<ColumnIdentifier, TableInputGenerator> relationalInputs = new HashMap<>();
        for (TableInputGenerator generator : inputGenerators) {
            try (RelationalInput input = generator.generateNewCopy()) {
                input.columnNames()
                        .forEach(columnName -> relationalInputs.put(
                                new ColumnIdentifier(input.relationName(), columnName), generator));
            } catch (Exception e) {
                throw new InputGenerationException(format("Error getting copy of %s", generator), e);
            }
        }
        return ImmutableMap.copyOf(relationalInputs);
    }

    private ImmutableMap<String, ColumnIdentifier> getIndexColumns(Map<ColumnIdentifier, TableInputGenerator> relationalInputs) {
        Map<String, ColumnIdentifier> indexColumns = new HashMap<>();
        ImmutableList<ColumnIdentifier> columns = relationalInputs.keySet().stream().sorted().collect(toImmutableList());
        for (ColumnIdentifier columnIdentifier : columns) {
            if (indexColumns.containsKey(columnIdentifier.getTableIdentifier())) {
                continue;
            }
            indexColumns.put(columnIdentifier.getTableIdentifier(), columnIdentifier);
        }
        return ImmutableMap.copyOf(indexColumns);
    }

    private ColumnIdentifier getIndexColumn(Map<String, ColumnIdentifier> indexColumns, ColumnIdentifier column)
            throws AlgorithmExecutionException {
        if (!indexColumns.containsKey(column.getTableIdentifier())) {
            throw new AlgorithmExecutionException(
                    format("Cannot find table %s in index columns", column.getTableIdentifier()));
        }
        return indexColumns.get(column.getTableIdentifier());
    }

    private Path getPath() throws FileCreationException {
        return config.getTempFileGenerator().getTemporaryFile().toPath();
    }

    private ColumnIdentifier getUnaryIdentifier(ColumnPermutation columnPermutation) {
        return getOnlyElement(columnPermutation.getColumnIdentifiers());
    }

    private void writeToFile(InclusionDependency uind, SetMultimap<Integer, Integer> uindCoordinates, Path path)
            throws IOException {
        ImmutableList<Integer> lhsCoordinates = uindCoordinates.keySet().stream()
                .sorted().collect(toImmutableList());
        try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
            for (Integer index : lhsCoordinates) {
                UindCoordinates coordinates = new UindCoordinates(uind, index, uindCoordinates.get(index));
                writer.write(coordinates.toLine());
                writer.newLine();
            }
        }
    }
}
