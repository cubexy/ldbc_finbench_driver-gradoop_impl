package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read5;

import java.util.List;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;

class ComplexRead5GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead5Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead5GradoopOperator(ComplexRead5 cr5) {
        this.id = cr5.getId();
        this.startTime = cr5.getStartTime().getTime();
        this.endTime = cr5.getEndTime().getTime();
        this.truncationLimit = cr5.getTruncationLimit();
        final TruncationOrder truncationOrder = cr5.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead5Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
