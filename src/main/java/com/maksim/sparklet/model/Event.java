package com.maksim.sparklet.model;

public record Event(int userId, String event, double amount) {
    public static Event parse(String line) {
        String[] p = line.split(",");
        if (p.length != 3) {
            throw new IllegalArgumentException("Bad line: " + line);
        }
        return new Event(Integer.parseInt(p[0].trim()), p[1].trim(), Double.parseDouble(p[2].trim()));
    }
}
