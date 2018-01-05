package de.metanome.util;

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.ArrayList;
import java.util.List;

public class InclusionDependencyResultReceiverStub implements InclusionDependencyResultReceiver {

    private final List<InclusionDependency> inds = new ArrayList<>();

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

    public List<InclusionDependency> getReceivedResults() {
        return inds;
    }
}
