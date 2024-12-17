package org.ldbcouncil.finbench.impls.gradoop;

import java.io.IOException;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbConnectionState;

public class GradoopFinbenchBaseGraphState extends DbConnectionState {

    private final TemporalGraph graph;
    private final boolean executeInCluster;

    public GradoopFinbenchBaseGraphState(TemporalGraph graph, boolean executeInCluster) {
        this.graph = graph;
        this.executeInCluster = executeInCluster;
    }

    public TemporalGraph getGraph() {
        return graph;
    }

    public boolean isExecutedInCluster() {
        return executeInCluster;
    }

    @Override
    public void close() throws IOException {
    }

}
