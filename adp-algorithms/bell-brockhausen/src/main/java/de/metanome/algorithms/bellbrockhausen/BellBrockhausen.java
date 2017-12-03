package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
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
        Set<InclusionDependency> candidates = generateCandidates(tableInfo);

        for(InclusionDependency candidate: candidates) {
            if(dataAccessObject.isValidUIND(candidate)) {
                configuration.getResultReceiver().receiveResult(candidate);
            }
        }
    }

    private Set<InclusionDependency> generateCandidates(TableInfo tableInfo) {
        Set<InclusionDependency> candidates = new HashSet<>();
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
