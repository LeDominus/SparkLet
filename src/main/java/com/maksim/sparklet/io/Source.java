package com.maksim.sparklet.io;

import java.io.IOException;
import java.util.List;

public interface Source<T> {
    List<List<T>> read() throws IOException;
}
