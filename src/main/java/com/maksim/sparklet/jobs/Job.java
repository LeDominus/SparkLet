package com.maksim.sparklet.jobs;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import com.maksim.sparklet.engine.Dataset;
import com.maksim.sparklet.io.Sink;
import com.maksim.sparklet.report.JobReport;
import com.maksim.sparklet.engine.TaskResult;
import com.maksim.sparklet.engine.Status;

public class Job<T> {
    private final String name;
    private final Dataset<T> data;
    private final Sink<T> sink;
    private final ExecutorService pool;

    public Job(String name, Dataset<T> data, Sink<T> sink, ExecutorService pool) {
        this.name = name;
        this.data = data;
        this.sink = sink;
        this.pool = pool;
    }

    public JobReport run() throws IOException {
        // Подготовка: started.at + sink.prepare();
        Instant startedAt = Instant.now();

        sink.prepare();


        boolean finalized = false;
        // Планирование: По задаче на партицию (callable)
        try {
            List<TaskResult> results = executeAll(data.partitions());
            
            boolean allSucceed = results.stream()
                .allMatch(r -> r.status() == Status.SUCCESS);
            
           Status jobStatus = allSucceed
            ? Status.SUCCESS
            : Status.FAILED;
            
            String failureReason = allSucceed
            ? null
            : "One or few partitions done with `FAILED`status";

            if (allSucceed) {
                sink.commit();
                finalized = true;
            } else {
                finalized = true;

                try {
                    sink.abort();
                } catch (RuntimeException e) {
                    String reason = e.getMessage();

                    if (reason == null || reason.isBlank()) {
                        reason = e.getClass().getSimpleName();
                    }

                    failureReason += "; abort failed: " + reason;
                }
            }

        Instant finishedAt = Instant.now();

        return new JobReport(
            name,
            startedAt,
            finishedAt,
            jobStatus,
            failureReason,
            results
        );
            
    } finally {
        if (!finalized) {
            abortQuietly();
        };
    }
}
    
    // Пока что синхронная реализация, далее подключить pool + Futures
    private List<TaskResult> executeAll(List<List<T>> partitions) {
        Objects.requireNonNull(partitions, "partitions");

        List<TaskResult> results = new ArrayList<>();

        for (int i = 0; i < partitions.size(); i++) {
            List<T> rows = partitions.get(i);

            TaskResult result = writeOne(i, rows);

            results.add(
                Objects.requireNonNull(
                    result, "writeOne returned null for partition " + i)
            );
        }
        return results;
    }

    private TaskResult writeOne(int index, List<T> rows) {
        Objects.requireNonNull(rows, "rows");

        try {
            sink.writeWithNPartition(index, rows);
            return new TaskResult(
                name,
                Status.SUCCESS,
                0.0,
                null
            );
        } catch (IOException | RuntimeException e) {
            String reason = e.getMessage();

            if (reason == null || reason.isBlank()) {
                reason = e.getClass().getSimpleName();
            }
            return new TaskResult(
                name,
                Status.FAILED,
                0.0,
                reason
            );
        }
    }

    private void abortQuietly() {
        try {
            sink.abort();
        } catch (RuntimeException ignored) {
            // Не заслоняю исходную ошибку
        }
    }
}
