package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;

class ComplexRead8GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead8Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold;

    public ComplexRead8GradoopOperator(ComplexRead8 cr8) {
        this.id = cr8.getId();
        this.startTime = cr8.getStartTime().getTime();
        this.endTime = cr8.getEndTime().getTime();
        this.truncationLimit = cr8.getTruncationLimit();
        final TruncationOrder truncationOrder = cr8.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold = cr8.getThreshold();
    }

    @Override
    public List<ComplexRead8Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
