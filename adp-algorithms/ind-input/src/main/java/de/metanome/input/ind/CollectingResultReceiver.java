package de.metanome.input.ind;

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class CollectingResultReceiver implements InclusionDependencyResultReceiver {

  private final List<InclusionDependency> received;

  CollectingResultReceiver() {
    received = new ArrayList<>();
  }

  @Override
  public void receiveResult(final InclusionDependency inclusionDependency) {
    received.add(inclusionDependency);
  }

  @Override
  public Boolean acceptedResult(InclusionDependency result) {
    return true;
  }

  List<InclusionDependency> getReceived() {
    return Collections.unmodifiableList(received);
  }
}
