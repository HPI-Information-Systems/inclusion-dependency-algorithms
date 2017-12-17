package de.metanome.algorithms.mind2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.mind2.configuration.Mind2Configuration;
import de.metanome.util.InclusionDependencyResultReceiverStub;
import de.metanome.util.RelationalInputGeneratorStub;
import de.metanome.util.Row;
import org.jukito.JukitoRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(JukitoRunner.class)
public class Mind2Test {

    private InclusionDependencyResultReceiverStub resultReceiver;
    @Inject private Mind2 mind2;

    @Before
    public void setupMocks(Mind2Configuration config) {
        resultReceiver = new InclusionDependencyResultReceiverStub();
        when(config.getResultReceiver()).thenReturn(resultReceiver);
    }

    @Test
    public void testPaperExample(Mind2Configuration config) throws AlgorithmExecutionException {
        // GIVEN
        ColumnIdentifier a1 = new ColumnIdentifier("R", "A1");
        ColumnIdentifier a2 = new ColumnIdentifier("R", "A2");
        ColumnIdentifier a3 = new ColumnIdentifier("R", "A3");
        ColumnIdentifier a4 = new ColumnIdentifier("R", "A4");
        ColumnIdentifier a5 = new ColumnIdentifier("R", "A5");
        ColumnIdentifier b1 = new ColumnIdentifier("S", "B1");
        ColumnIdentifier b2 = new ColumnIdentifier("S", "B2");
        ColumnIdentifier b3 = new ColumnIdentifier("S", "B3");
        ColumnIdentifier b4 = new ColumnIdentifier("S", "B4");
        ColumnIdentifier b5 = new ColumnIdentifier("S", "B5");

        RelationalInputGenerator tableR = RelationalInputGeneratorStub.builder()
                .columnNames(ImmutableList.of("A1", "A2", "A3", "A4", "A5"))
                .rows(ImmutableList.of(
                        Row.of("a", "b", "c", "d", "e"),
                        Row.of("f", "g", "i", "j", "k")))
                .build();
        RelationalInputGenerator tableS = RelationalInputGeneratorStub.builder()
                .columnNames(ImmutableList.of("B1", "B2", "B3", "B4", "B5"))
                .rows(ImmutableList.of(
                        Row.of("a", "b", "c", "d", null),
                        Row.of(null, null, "c", "d", null),
                        Row.of(null, null, "c", "d", "e"),
                        Row.of("f", "g", "i", null, null),
                        Row.of("f", "g", "i", null, null),
                        Row.of("f", "g", null, "j", "k")))
                .build();
        ImmutableSet<InclusionDependency> unaryInds = ImmutableSet.of(
                toInd(a1, b1), toInd(a2, b2), toInd(a3, b3), toInd(a4, b4), toInd(a5, b5));
        ImmutableSet<InclusionDependency> maximumInds = ImmutableSet.of(
                new InclusionDependency(new ColumnPermutation(a1, a2, a3), new ColumnPermutation(b1, b2, b3)),
                new InclusionDependency(new ColumnPermutation(a1, a2, a4), new ColumnPermutation(b1, b2, b4)),
                new InclusionDependency(new ColumnPermutation(a4, a5), new ColumnPermutation(b4, b5)));

        when(config.getRelationalInputGenerators()).thenReturn(ImmutableList.of(tableR, tableS));
        when(config.getUnaryInds()).thenReturn(unaryInds);

        // WHEN
        mind2.execute();

        // THEN
        assertThat(resultReceiver.getReceivedResults()).containsAll(maximumInds);
    }

    private InclusionDependency toInd(ColumnIdentifier dependant, ColumnIdentifier referenced) {
        return new InclusionDependency(new ColumnPermutation(dependant), new ColumnPermutation(referenced));
    }
}
