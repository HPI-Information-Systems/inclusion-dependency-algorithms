package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static de.metanome.util.Collectors.toImmutableList;
import static de.metanome.util.Collectors.toImmutableSet;

public class BellBrockhausen {

    private final BellBrockhausenConfiguration configuration;
    private final DataAccessObject dataAccessObject;

    @Inject
    public BellBrockhausen(BellBrockhausenConfiguration configuration, DataAccessObject dataAccessObject) {
        this.configuration = configuration;
        this.dataAccessObject = dataAccessObject;
    }

    public void execute() throws AlgorithmExecutionException {
        ImmutableSet<Attribute> attributes = collectAttributes();
        Set<Attribute> candidates = generateCandidates(attributes);

        IndGraph indGraph = new IndGraph(
                configuration.getResultReceiver(),
                dataAccessObject,
                ImmutableList.copyOf(candidates));

        indGraph.testCandidates();
    }

    private Set<Attribute> generateCandidates(ImmutableSet<Attribute> attributes) {
        Set<Attribute> candidates = new HashSet<>();
        for (Attribute attributeA : attributes) {
            for (Attribute attributeB : attributes) {
                if (attributeA.getColumnIdentifier().equals(attributeB.getColumnIdentifier()) ||
                        !attributeA.getDataType().equals(attributeB.getDataType())) {
                    continue;
                }
                if (attributeA.getValueRange().encloses(attributeB.getValueRange()) ||
                        attributeB.getValueRange().encloses(attributeA.getValueRange())) {
                    candidates.add(attributeA);
                    candidates.add(attributeB);
                }
            }
        }
        return candidates;
    }

    private ImmutableSet<Attribute> collectAttributes() throws AlgorithmExecutionException {
        List<TableInfo> tableInfos = new ArrayList<>();
        for (String tableName : configuration.getTableNames()) {
            tableInfos.add(dataAccessObject.getTableInfo(tableName));
        }
        return tableInfos.stream()
                .map(TableInfo::getAttributes)
                .flatMap(List::stream)
                .collect(toImmutableSet());
    }
}
