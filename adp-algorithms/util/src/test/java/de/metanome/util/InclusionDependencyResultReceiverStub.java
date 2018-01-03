package de.metanome.util;

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.HashSet;
import java.util.Set;

public class InclusionDependencyResultReceiverStub implements InclusionDependencyResultReceiver {

    private final Set<InclusionDependency> inds = new HashSet<>();

    @Override
    public void receiveResult(InclusionDependency inclusionDependency) {
        inds.add(inclusionDependency);
    }

    @Override
    public Boolean acceptedResult(InclusionDependency result) {
        return null;
    }

    public boolean hasReceivedResult(InclusionDependency inclusionDependency) {
        return inds.contains(inclusionDependency);
    }

    public Set<InclusionDependency> getReceivedResults() {
        return inds;
    }
}
