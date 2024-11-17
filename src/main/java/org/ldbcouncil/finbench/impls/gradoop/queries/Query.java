package org.ldbcouncil.finbench.impls.gradoop.queries;

import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;

public class Query {
    private final String queryName;
    private final UnaryBaseGraphToValueOperator<TemporalGraph, ?> queryOperator;
    private final ArgArrayToInputDataTuple argArrayToInputDataTuple;

    public Query(String queryName,
                 UnaryBaseGraphToValueOperator<TemporalGraph, ?> queryOperator,
                 ArgArrayToInputDataTuple argArrayToInputDataTuple
    ) {
        this.queryName = queryName;
        this.queryOperator = queryOperator;
        this.argArrayToInputDataTuple = argArrayToInputDataTuple;
    }

    public String getQueryName() {
        return queryName;
    }

    public UnaryBaseGraphToValueOperator<TemporalGraph, ?> getQueryOperator() {
        return queryOperator;
    }

    public ArgArrayToInputDataTuple getArgArrayToInputDataTuple() {
        return argArrayToInputDataTuple;
    }
}
