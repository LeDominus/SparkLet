package com.maksim.sparklet.engine;

public record TaskResult(String taskName, boolean success, long durationMs) {}
