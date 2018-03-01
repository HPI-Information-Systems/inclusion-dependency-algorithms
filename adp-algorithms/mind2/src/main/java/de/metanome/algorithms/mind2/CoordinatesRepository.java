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
import de.metanome.algorithms.mind2.model.ValuePositions;
import de.metanome.algorithms.mind2.utils.AttributeIterator;
import de.metanome.algorithms.mind2.utils.UindCoordinatesReader;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getOnlyElement;
import static de.metanome.util.Collectors.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class CoordinatesRepository {

    private static final Logger log = Logger.getLogger(CoordinatesRepository.class.getName());

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
            log.info(format("Calculate coordinates for %s", uind));
            List<ValuePositions> uindCoordinates = generateCoordinates(uind, attributes, indexColumns);
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

    private List<ValuePositions> generateCoordinates(
            InclusionDependency unaryInd,
            ImmutableMap<ColumnIdentifier, TableInputGenerator> attributes,
            ImmutableMap<String, ColumnIdentifier> indexColumns) throws AlgorithmExecutionException {
        List<ValuePositions> valuePositions = new ArrayList<>();
        ColumnIdentifier lhs = getUnaryIdentifier(unaryInd.getDependant());
        ColumnIdentifier rhs = getUnaryIdentifier(unaryInd.getReferenced());

        RelationalInput inputA = config.getDataAccessObject()
                .getSortedRelationalInput(attributes.get(lhs), lhs, getIndexColumn(indexColumns, lhs), config.getIndexColumn(), false);
        RelationalInput inputB = config.getDataAccessObject().
                getSortedRelationalInput(attributes.get(rhs), rhs, getIndexColumn(indexColumns, rhs), config.getIndexColumn(), false);
        AttributeIterator cursorA =  new AttributeIterator(inputA, lhs, config.getIndexColumn());
        AttributeIterator cursorB = new AttributeIterator(inputB, rhs, config.getIndexColumn());
        String previousValue = null;

        cursorA.next();
        cursorB.next();

        while (cursorA.hasNext() || cursorB.hasNext()) {
            AttributeValuePosition valA = cursorA.current();
            AttributeValuePosition valB = cursorB.current();
            previousValue = valA.getValue();

            if (valA.getValue().equals(valB.getValue())) {
                List<Integer> positionsA = new ArrayList<>();
                List<Integer> positionsB = new ArrayList<>();

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
                    valuePositions.add(new ValuePositions(positionsA, positionsB));
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
        if (!cursorA.current().getValue().equals(previousValue) &&
                cursorA.current().getValue().equals(cursorB.current().getValue())) {
            valuePositions.add(new ValuePositions(cursorA.current().getPosition(), cursorB.current().getPosition()));
        }
        cursorA.close();
        cursorB.close();
        return valuePositions;
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

    private void writeToFile(InclusionDependency uind, List<ValuePositions> uindCoordinates, Path path)
            throws IOException {
        log.info("Dump to file: " + uind);
        try (BufferedWriter writer = Files.newBufferedWriter(path, UTF_8)) {
            for (ValuePositions valuePositions : uindCoordinates) {
                List<Integer> lhsIndices = valuePositions.getPositionsA().stream().sorted().collect(toList());
                String rhsString = UindCoordinates.toRhsLine(valuePositions.getPositionsB());
                for (int lhsIndex : lhsIndices) {
                    writer.write(UindCoordinates.toLine(lhsIndex, rhsString));
                    writer.newLine();
                }

            }
        }
        log.info("Dump to file finished");
    }
}
