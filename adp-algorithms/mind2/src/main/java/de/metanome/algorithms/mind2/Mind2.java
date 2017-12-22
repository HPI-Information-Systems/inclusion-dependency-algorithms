package de.metanome.algorithms.mind2;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.algorithms.mind2.utils.UindCoordinates;
import de.metanome.algorithms.mind2.utils.UindCoordinatesReader;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static de.metanome.algorithms.mind2.utils.IndComparators.UindCoordinatesReaderComparator;

public class Mind2 {

    private final CoordinatesRepository repository = new CoordinatesRepository();
    private final Mind2Configuration configuration;

    @Inject
    public Mind2(Mind2Configuration configuration) {
        this.configuration = configuration;
    }

    public void execute() throws AlgorithmExecutionException {
        repository.storeUindCoordinates(configuration);

        System.out.println("Done exec");
    }
}
