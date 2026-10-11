package com.maksim.sparklet.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

public class Dataset<T> {
    private final List<List<T>> partitions;
    private final ExecutorService pool;

    private Dataset(List<List<T>> partitions, ExecutorService pool) {
        this.partitions = partitions;
        this.pool = pool;
    }

    public static <T> Dataset<T> of(List<List<T>> partitions, ExecutorService pool) {
        List<List<T>> copy = new ArrayList<>();
        for (List<T> p : partitions) {
            copy.add(List.copyOf(p));
        }
        return new Dataset<>(List.copyOf(copy), pool);
    }

    public <R> Dataset<R> map(Function<T, R> f) {
        return mapPartitions(part -> {
            List<R> out = new ArrayList<>(part.size());
            for (T item : part) {
                out.add(f.apply(item));
            }
            return out;
        });
    }

    public Dataset<T> filter(Predicate<T> p) {
        return mapPartitions(part -> {
            List<T> out = new ArrayList<>();
            for (T item : part) {
                if (p.test(item)) {
                    out.add(item);
                }
            }
            return out;
        });
    }
    
    private <R> Dataset<R> mapPartitions(Function<List<T>, List<R>> f) {
        List<Callable<List<R>>> jobs = new ArrayList<>();
        for (List<T> part : partitions) {
            jobs.add(() -> f.apply(part));
        }

        List<List<R>> result = new ArrayList<>();
        try {
            for (Future<List<R>> future : pool.invokeAll(jobs)) {
                result.add(List.copyOf(future.get()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while processing partitions", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Partition processing failed", e.getCause());
        }
        return new Dataset<>(List.copyOf(result), pool);
    }

    public List<List<T>> partitions() {
        return partitions;
    }

    public List<T> collect() {
        List<T> all = new ArrayList<>();
        for (List<T> p : partitions) {
            all.addAll(p);
        }
        return all;
    }

    public long count() {
        return partitions.stream().mapToLong(List::size).sum();
    }
}
