package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.accessors.TableInfo;
import de.metanome.algorithms.bellbrockhausen.accessors.DataAccessObject;
import de.metanome.algorithms.bellbrockhausen.configuration.BellBrockhausenConfiguration;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import java.util.ArrayList;
import java.util.List;

public class BellBrockhausen {

    private final BellBrockhausenConfiguration configuration;
    private final DataAccessObject dataAccessObject;

    public BellBrockhausen(BellBrockhausenConfiguration configuration, DataAccessObject dataAccessObject) {
        this.configuration = configuration;
        this.dataAccessObject = dataAccessObject;
    }

    public void execute() throws AlgorithmExecutionException {
        TableInfo tableInfo = dataAccessObject.getTableInfo(configuration.getTableName());
        List<InclusionDependency> candidates = generateCandidates(tableInfo);
        /* TODO: Check if the candidates fulfill the IND requierements */
        for(InclusionDependency candidate: candidates) {
            configuration.getResultReceiver().receiveResult(candidate);
        }
    }

    private List<InclusionDependency> generateCandidates(TableInfo tableInfo) {
        List<InclusionDependency> candidates = new ArrayList<>();
        for (Attribute attributeA : tableInfo.getAttributes()) {
            for (Attribute attributeB : tableInfo.getAttributes()) {
                if (attributeA.equals(attributeB)) continue;
                if (attributeA.getValueRange().encloses(attributeB.getValueRange())) {
                    candidates.add(new InclusionDependency(
                            new ColumnPermutation(attributeB.getColumnIdentifier()),
                            new ColumnPermutation(attributeA.getColumnIdentifier())));
                }
            }
        }
        return candidates;
    }
}
