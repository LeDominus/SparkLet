package com.maksim.sparklet.io;

import java.io.IOException;
import java.util.List;
import com.maksim.sparklet.engine.Dataset;

public interface Sink<T> {    
    void prepare() throws IOException;
    void writeWithNPartition(int index, List<T> rows) throws IOException;
    void writeWithNPartitions(Dataset<T> ds, Sink<T> sink) throws IOException;
    void commit() throws IOException;
    void abort();
}
