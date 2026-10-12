package com.maksim.sparklet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.maksim.sparklet.model.*;
import com.maksim.sparklet.io.*;
import com.maksim.sparklet.jobs.*;
import com.maksim.sparklet.engine.*;
import com.maksim.sparklet.report.*;;

public class Main {
    public static void main(String[] args) throws IOException{
        ExecutorService pool = Executors.newFixedThreadPool(4);
        final String input_dir = "data/raw/events.csv";
        final String jobName = "purchases_v2";
        final String output_dir = "data/result/" + jobName;
        final String header = "user_id,amount,bucket";

        try {
            // READ DATA
            Source<Event> source = new csvSource<>(Path.of(input_dir), 4, Event::parse);
            Dataset<Event> events = Dataset.of(source.read(), pool);

            // TRANSFORM DATA
            Dataset<PurchaseRow> purchases = PurchaseJob.build(events);
            
            // CREATE SINK
            Sink<PurchaseRow> sink = new csvSink<>(
                Path.of(output_dir),
                header,
                r -> r.userId() + "," + r.amount() + "," + r.bucket()
            );
            
            // CREATE JOB
            Job<PurchaseRow> job = new Job<>(
                "purchases-job",
                purchases,
                sink,
                pool
            );

            // EXECUTE JOB
            JobReport report = job.run();

            System.out.println("Job: " + report.name());
            System.out.println("Status: " + report.status());
            System.out.println("Duration: " + report.duration());

            if (report.failureReason() != null) {
                System.err.println("Failure" + report.failureReason());
            }

            System.out.println(
                "Partitions processed: " + report.taskResults().size()
            );

        } finally {
            pool.shutdown();
        }
    }
}