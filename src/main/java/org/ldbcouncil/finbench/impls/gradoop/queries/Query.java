package org.ldbcouncil.finbench.impls.gradoop.queries;

import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;

public class Query {
    public Query(OperationHandler<?, GradoopFinbenchBaseGraphState> queryHandler,
                 UnaryBaseGraphToValueOperator<TemporalGraph, ?> queryOperator) {
    }
}
