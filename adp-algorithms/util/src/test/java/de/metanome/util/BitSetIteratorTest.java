package de.metanome.util;

import static com.google.common.primitives.Ints.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class BitSetIteratorTest {

  @Test
  void emptyHasNoNext() {
    final BitSetIterator iterator = newIterator(Collections.emptyList());

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void testIteration() {
    final List<Integer> on = asList(3, 5, 7, 10);
    final BitSetIterator iterator = newIterator(on);

    final List<Integer> collected = new ArrayList<>();
    while (iterator.hasNext()) {
      collected.add(iterator.next());
    }

    assertThat(collected).isEqualTo(on);
  }

  private BitSetIterator newIterator(final Collection<Integer> on) {
    final BitSet set = new BitSet(on.isEmpty() ? 5 : Collections.max(on));
    on.forEach(set::set);
    return BitSetIterator.of(set);
  }

}