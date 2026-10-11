package com.maksim.sparklet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.maksim.sparklet.engine.Dataset;
import com.maksim.sparklet.io.csvSink;
import com.maksim.sparklet.io.csvSource;
import com.maksim.sparklet.io.Sink;
import com.maksim.sparklet.io.Source;
import com.maksim.sparklet.jobs.PurchaseJob;
import com.maksim.sparklet.model.Event;
import com.maksim.sparklet.model.PurchaseRow;

public class Main {
    public static void main(String[] args) throws IOException{
        ExecutorService pool = Executors.newFixedThreadPool(4);
        final String input_dir = "data/raw/events.csv";
        final String output_dir = "data/result/purchases";
        final String header = "user_id,amount,bucket";

        try {
            // READ DATA
            Source<Event> source = new csvSource<>(Path.of(input_dir), 4, Event::parse);
            Dataset<Event> events = Dataset.of(source.read(), pool);

            // TRANSFORM DATA
            Dataset<PurchaseRow> purchases = PurchaseJob.build(events);
            
            // WRITE
            Sink<PurchaseRow> sink = new csvSink<>(
                Path.of(output_dir),
                header,
                r -> r.userId() + "," + r.amount() + "," + r.bucket()
            );
            sink.writeWithNPartitions(purchases, sink);   
        } finally {
            pool.shutdown();
        }
    }

}