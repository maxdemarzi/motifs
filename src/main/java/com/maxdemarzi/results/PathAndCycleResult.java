package com.maxdemarzi.results;

import java.util.List;

public class PathAndCycleResult {
    public final String input;
    public final List<Object> value;
    public final String cycle;

    public PathAndCycleResult(String input, List<Object> value, String cycle) {
        this.input = input;
        this.value = value;
        this.cycle = cycle;
    }
}
