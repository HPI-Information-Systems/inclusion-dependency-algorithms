package de.metanome.algorithms.bellbrockhausen;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.HashSet;
import java.util.Set;

public class IndBaseTest {

    public InclusionDependencyResultReceiver getResultReciever() {
        return new InclusionDependencyResultReceiver() {

            private Set<InclusionDependency> inds = new HashSet<>();

            @Override
            public void receiveResult(InclusionDependency inclusionDependency) {
                inds.add(inclusionDependency);
            }

            @Override
            public Boolean acceptedResult(InclusionDependency result) {
                return inds.contains(result);
            }

            public Set<InclusionDependency> getInds() {
                return inds;
            }
        };
    }

    public InclusionDependency toInd(ColumnIdentifier dependant, ColumnIdentifier referenced) {
        return new InclusionDependency(new ColumnPermutation(dependant), new ColumnPermutation(referenced));
    }
}
