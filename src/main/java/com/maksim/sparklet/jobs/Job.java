package com.maksim.sparklet.jobs;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
    
    private List<TaskResult> executeAll(List<List<T>> partitions) {
    Objects.requireNonNull(partitions, "partitions");

    List<Callable<TaskResult>> tasks = new ArrayList<>();

    for (int i = 0; i < partitions.size(); i++) {
        final int partitionIndex = i;
        List<T> rows = partitions.get(i);

        Callable<TaskResult> task =
            () -> writeOne(partitionIndex, rows);

        tasks.add(task);
    }

    List<Future<TaskResult>> futures;

    try {
        futures = pool.invokeAll(tasks);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Job execution was interrupted", e);
    }

    List<TaskResult> results = new ArrayList<>(futures.size());

    for (int i = 0; i < futures.size(); i++) {
        Future<TaskResult> future = futures.get(i);

        try {
            TaskResult result = future.get();

            results.add(
                Objects.requireNonNull(
                    result,
                    "Task returned null for partition " + i
                )
            );

        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null
                ? e.getCause()
                : e;

            String reason = cause.getMessage();

            if (reason == null || reason.isBlank()) {
                reason = cause.getClass().getSimpleName();
            }

            results.add(new TaskResult(
                name,
                Status.FAILED,
                0.0,
                reason
            ));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                "Interrupted while collecting partition results", e
            );
        }
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
