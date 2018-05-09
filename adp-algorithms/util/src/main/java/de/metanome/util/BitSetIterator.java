package de.metanome.util;

import java.util.BitSet;

/**
 * Alternative to the complex and error-prone for-loop suggested in the {@code BitSet} documentation.
 *
 * <p>Cannot possibly implement {@code Iterator<Integer>} since the required boxing would certainly
 * have an negative impact on performance.</p>
 *
 * @see BitSet#nextSetBit(int)
 */
public class BitSetIterator {

  private final BitSet set;
  private int position;

  BitSetIterator(final BitSet set, final int from) {
    this.set = set;
    position = set.nextSetBit(from);
  }

  public boolean hasNext() {
    return position >= 0;
  }

  public int next() {
    final int current = position;
    position = set.nextSetBit(position + 1);
    return current;
  }

  public static BitSetIterator of(final BitSet set) {
    return new BitSetIterator(set, 0);
  }
}
