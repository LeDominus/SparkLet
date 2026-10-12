package com.maksim.sparklet.report;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.maksim.sparklet.engine.*;

public record JobReport (
    String name,
    Instant startedAt,
    Instant finishedAt,
    Status status,
    String failureReason,
    List<TaskResult> taskResults
) {

    public JobReport(
        String name,
        Instant startedAt,
        Instant finishedAt,
        Status status,
        String failureReason,
        List<TaskResult> taskResults
    ) {
        this.name = name;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.status = status;
        this.failureReason = "";
        this.taskResults = taskResults;
    }

    public Duration duration() {
        return Duration.between(startedAt, finishedAt);
    }
}
