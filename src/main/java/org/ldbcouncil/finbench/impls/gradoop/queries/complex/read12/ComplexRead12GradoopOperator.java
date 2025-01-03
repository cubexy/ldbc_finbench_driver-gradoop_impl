package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;

class ComplexRead12GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead12Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead12GradoopOperator(ComplexRead12 cr12) {
        this.id = cr12.getId();
        this.startTime = cr12.getStartTime().getTime();
        this.endTime = cr12.getEndTime().getTime();
        this.truncationLimit = cr12.getTruncationLimit();
        final TruncationOrder truncationOrder = cr12.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead12Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
