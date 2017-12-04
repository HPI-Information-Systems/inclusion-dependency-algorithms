package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import java.util.HashSet;
import java.util.Set;

public class BellBrockhausen {

    private final BellBrockhausenConfiguration configuration;
    private final DataAccessObject dataAccessObject;

    public BellBrockhausen(BellBrockhausenConfiguration configuration, DataAccessObject dataAccessObject) {
        this.configuration = configuration;
        this.dataAccessObject = dataAccessObject;
    }

    public void execute() throws AlgorithmExecutionException {
        TableInfo tableInfo = dataAccessObject.getTableInfo(configuration.getTableName());
        Set<ColumnIdentifier> candidates = generateCandidates(tableInfo);

        IndGraph indGraph = new IndGraph(
                configuration.getResultReceiver(),
                dataAccessObject,
                ImmutableList.copyOf(candidates));

        indGraph.testCandidates();
    }

    private Set<ColumnIdentifier> generateCandidates(TableInfo tableInfo) {
        Set<ColumnIdentifier> candidates = new HashSet<>();
        for (Attribute attributeA : tableInfo.getAttributes()) {
            for (Attribute attributeB : tableInfo.getAttributes()) {
                if (attributeA.equals(attributeB)) continue;
                if (attributeA.getValueRange().encloses(attributeB.getValueRange()) ||
                        attributeB.getValueRange().encloses(attributeA.getValueRange())) {
                    candidates.add(attributeA.getColumnIdentifier());
                    candidates.add(attributeB.getColumnIdentifier());
                }
            }
        }
        return candidates;
    }
}
