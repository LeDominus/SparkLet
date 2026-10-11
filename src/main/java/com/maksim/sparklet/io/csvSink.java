package com.maksim.sparklet.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import com.maksim.sparklet.engine.Dataset;

public class csvSink<T> implements Sink<T> {
    private final Path dir;
    private final String header;
    private final Function<T, String> formatter;

    public csvSink(Path dir, String header, Function<T, String> formatter) {
        this.dir = dir;
        this.header = header;
        this.formatter = formatter;
    }

    @Override
    public void writeWithNPartition(int index, List<T> rows) {
        String fileName = "part-%05d.csv".formatted(index);
        Path tmp = dir.resolve("." + fileName + ".tmp");
        Path target = dir.resolve(fileName);

        try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            w.write(header);
            w.newLine();
            for (T row : rows) {
                w.write(formatter.apply(row));
                w.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

    @Override 
    public void writeWithNPartitions(Dataset<T> ds, Sink<T> sink) throws IOException {
        sink.prepare();
        try {
            List<List<T>> parts = ds.partitions();
            for (int i = 0; i < parts.size(); i++) {
                sink.writeWithNPartition(i, parts.get(i));
            }
            sink.commit();
        } catch (IOException | RuntimeException e) {
            sink.abort();
            throw e;
        }
    }

    @Override 
    public void prepare() throws IOException {
        if (Files.exists(dir)) {
            throw new FileAlreadyExistsException(dir.toString());
        }
        Files.createDirectories(dir);
    }

    @Override 
    public void commit() throws IOException {
        Path successPath = dir.resolve("_SUCCESS");

        Files.deleteIfExists(successPath);
        Files.createFile(successPath);
        System.out.println("--- All partition successfully written. _SUCCESS file has been created ---");
    }

    @Override
    public void abort() {
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder())
                .forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
            // Откат не маскирует ошибку
        }
    }
}
