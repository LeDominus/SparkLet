package com.maksim.sparklet.engine;

public record TaskResult(
    String taskName,
    Status status,
    long durationMs,
    String error
) {

    public TaskResult(String name, Status success, double d, String string) {
        this(name, success, 0, string);
    }}
