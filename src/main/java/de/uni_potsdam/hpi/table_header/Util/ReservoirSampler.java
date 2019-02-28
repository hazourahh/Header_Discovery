package de.uni_potsdam.hpi.table_header.Util;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ReservoirSampler<T> implements Collector<T, List<T>, List<T>> {
    final Random rand = new SecureRandom();
    final int sz;
    int c = 0;

    public ReservoirSampler(int size) {
        this.sz = size;
    }

    private void addIt(final List<T> in, T s) {
        if (in.size() < sz) {
            in.add(s);
        }
        else {
            int replaceInIndex = (int) (rand.nextDouble() * (sz + (c++) + 1));
            if (replaceInIndex < sz) {
                in.set(replaceInIndex, s);
            }
        }
    }

    @Override
    public Supplier<List<T>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<T>, T> accumulator() {
        return this::addIt;
    }

    @Override
    public BinaryOperator<List<T>> combiner() {
        return (left, right) -> {
            left.addAll(right);
            return left;
        };
    }

    @Override
    public Set<java.util.stream.Collector.Characteristics> characteristics() {
        return EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
    }

    @Override
    public Function<List<T>, List<T>> finisher() {
        return (i) -> i;
    }

}


