package io.docsql;

import java.util.ArrayList;
import java.util.List;

/** One statement outcome: result-set columns/rows or an affected-row count. */
public final class Result {

    public final List<String> columns = new ArrayList<>();
    public final List<Object[]> rows = new ArrayList<>();
    public long rowcount = -1;

    public boolean hasResultSet() {
        return rowcount < 0;
    }

    /** Long value of the single cell of a one-row/one-column result, -1 when absent. */
    long singleLong() {
        if (rows.size() == 1 && rows.get(0).length == 1 && rows.get(0)[0] instanceof Long) {
            return (Long) rows.get(0)[0];
        }
        return -1;
    }
}
