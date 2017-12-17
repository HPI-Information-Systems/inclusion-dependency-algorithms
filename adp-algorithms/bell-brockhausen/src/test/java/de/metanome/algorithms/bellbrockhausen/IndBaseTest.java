package de.metanome.algorithms.bellbrockhausen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.bellbrockhausen.models.Attribute;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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

    public void recievedAllValidInds(
            InclusionDependencyResultReceiver resultReceiver,
            ImmutableList<Attribute> attributes,
            ImmutableSet<InclusionDependency> validInds) {
        for (Attribute attrA : attributes) {
            for (Attribute attrB : attributes) {
                InclusionDependency ind = toInd(attrA.getColumnIdentifier(), attrB.getColumnIdentifier());
                boolean isValidInd = validInds.contains(ind);
                assertThat(resultReceiver.acceptedResult(ind)).isEqualTo(isValidInd);
            }
        }
    }
}
