package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;

class ComplexRead7GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead7Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold;

    public ComplexRead7GradoopOperator(ComplexRead7 cr7) {
        this.id = cr7.getId();
        this.startTime = cr7.getStartTime().getTime();
        this.endTime = cr7.getEndTime().getTime();
        this.truncationLimit = cr7.getTruncationLimit();
        final TruncationOrder truncationOrder = cr7.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold = cr7.getThreshold();
    }

    @Override
    public List<ComplexRead7Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
