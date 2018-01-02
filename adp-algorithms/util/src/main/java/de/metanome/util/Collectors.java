package de.metanome.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.stream.Collector;

import static java.util.stream.Collector.Characteristics.UNORDERED;

public class Collectors {

    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> toImmutableList() {
        return Collector.of(ImmutableList.Builder<T>::new,
                ImmutableList.Builder<T>::add,
                (l, r) -> l.addAll(r.build()),
                ImmutableList.Builder<T>::build);
    }

    public static <T> Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>> toImmutableSet() {
        return Collector.of(ImmutableSet.Builder<T>::new,
                ImmutableSet.Builder<T>::add,
                (l, r) -> l.addAll(r.build()),
                ImmutableSet.Builder<T>::build, UNORDERED);
    }
}
