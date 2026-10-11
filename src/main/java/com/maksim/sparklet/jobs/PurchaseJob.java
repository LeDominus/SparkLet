package com.maksim.sparklet.jobs;

import com.maksim.sparklet.engine.Dataset;
import com.maksim.sparklet.model.Event;
import com.maksim.sparklet.model.PurchaseRow;


public final class PurchaseJob {
    private PurchaseJob() {}

    public static Dataset<PurchaseRow> build(Dataset<Event> events) {
        return events
            .filter(e -> e.amount() > 0)
            .map(PurchaseRow::from);
    }
}
