package org.ldbcouncil.finbench.impls.gradoop;

import java.io.IOException;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbConnectionState;

public class GradoopFinbenchBaseGraphState extends DbConnectionState {

    private final TemporalGraph graph;

    public GradoopFinbenchBaseGraphState(TemporalGraph graph) {
        this.graph = graph;
    }

    public TemporalGraph getGraph() {
        return graph;
    }

    @Override
    public void close() throws IOException {
    }

}
