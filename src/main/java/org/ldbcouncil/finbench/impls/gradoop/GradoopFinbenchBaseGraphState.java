package org.ldbcouncil.finbench.impls.gradoop;

import java.io.IOException;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbConnectionState;

public class GradoopFinbenchBaseGraphState extends DbConnectionState {

    private final TemporalGraph graph;
    private final boolean useFlinkSort;
    private final int parallelism;

    public GradoopFinbenchBaseGraphState(TemporalGraph graph, boolean useFlinkSort, int parallelism) {
        this.graph = graph;
        this.useFlinkSort = useFlinkSort;
        this.parallelism = parallelism;
    }

    public TemporalGraph getGraph() {
        return graph;
    }

    public boolean isFlinkSort() {
        return useFlinkSort;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void close() throws IOException {
    }

}
