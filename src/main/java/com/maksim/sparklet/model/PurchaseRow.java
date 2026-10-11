package com.maksim.sparklet.model;

public record PurchaseRow(int userId, double amount, String bucket) {
    public static PurchaseRow from(Event e) {
        return new PurchaseRow(e.userId(), e.amount(), e.amount() >= 100 ? "high" : "low");
    }
}
