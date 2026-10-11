package com.maksim.sparklet.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class csvSource<T> implements Source<T>{
    private final Path path;
    private final int n_partitions;
    private final Function<String, T> parser;

    public csvSource(Path path, int n_partitions, Function<String, T> parser) {
        this.path = path;
        this.n_partitions = n_partitions;
        this.parser = parser;
    }

    @Override
    public List<List<T>> read() throws IOException {
        List<T> rows;
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            rows = lines.skip(1) // ПРопускаю заголовок
                .filter(l -> !l.isBlank())
                .map(parser)
                .toList();
        } 
        return split(rows, n_partitions);
    }

    static <T> List<List<T>> split(List<T> rows, int n) {
        int base = rows.size() / n;
        int extra = rows.size() % n;
        List<List<T>> parts = new ArrayList<>(n);
        int from = 0;
        for (int i = 0; i < n; i++) {
            int size = base + (i < extra ? 1 : 0);
            parts.add(rows.subList(from, from + size));
            from += size;
        }
        return parts;
    }
}
