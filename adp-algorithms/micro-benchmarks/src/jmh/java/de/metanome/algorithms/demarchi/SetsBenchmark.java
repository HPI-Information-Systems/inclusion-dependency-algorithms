package de.metanome.algorithms.demarchi;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.BitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Setup: Create k sets of type X. Each set has exactly one element less than its predecessor.
 * Test: Create a copy of the first set and measure how long it takes to intersect all remaining sets.
 */
@Fork(warmups = 1, value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
public class SetsBenchmark {

  private static final int ITEM_COUNT = 10_000;
  private static final int SET_COUNT = 10_000;

  private IntSet[] intSets;
  private BitSet[] bitSets;

  @Setup
  public void setUp() {
    intSets = new IntSet[SET_COUNT];
    bitSets = new BitSet[SET_COUNT];

    for (int index = 0; index < SET_COUNT; ++index) {

      intSets[index] = new IntOpenHashSet(ITEM_COUNT);
      bitSets[index] = new BitSet(ITEM_COUNT);

      for (int k = index; k < ITEM_COUNT; ++k) {
        intSets[index].add(k);
        bitSets[index].set(k);
      }
    }
  }

  @Benchmark
  public void testIntSets(final Blackhole blackhole) {
    final IntSet set = new IntOpenHashSet(intSets[0]);

    for (int index = 1; index < SET_COUNT; ++index) {
      set.retainAll(intSets[index]);
    }

    blackhole.consume(set);
    checkLength(set.size());
  }

  @Benchmark
  public void testBitSets(final Blackhole blackhole) {
    final BitSet set = new BitSet(ITEM_COUNT);
    set.or(bitSets[0]);

    for (int index = 1; index < SET_COUNT; ++index) {
      set.and(bitSets[index]);
    }

    blackhole.consume(set);
    checkLength(set.cardinality());
  }

  private void checkLength(final int length) {
    if (length != 1) {
      throw new IllegalStateException("Expected only one remaining element but was " + length);
    }
  }
}
