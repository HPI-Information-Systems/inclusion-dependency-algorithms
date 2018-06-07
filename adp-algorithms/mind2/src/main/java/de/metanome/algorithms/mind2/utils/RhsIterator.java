package de.metanome.algorithms.mind2.utils;

import com.google.common.collect.AbstractIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

public class RhsIterator extends AbstractIterator<Integer> {

    private final int uindId;
    private IntIterator iter;
    private int current;

    public RhsIterator(Integer uindId, IntList iter) {
        this.uindId = uindId;
        this.iter = iter.iterator();
        next();
    }

    @Override
    protected Integer computeNext() {
        if (iter.hasNext()) {
            Integer result = current;
            current = iter.nextInt();
            return result;
        }
        return endOfData();
    }

    public int current() {
        return current;
    }

    public int getUindId() {
        return uindId;
    }
}
