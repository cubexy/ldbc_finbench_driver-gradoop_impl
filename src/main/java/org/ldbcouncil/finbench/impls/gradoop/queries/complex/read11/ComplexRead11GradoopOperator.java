package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;

class ComplexRead11GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead11Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;

    public ComplexRead11GradoopOperator(ComplexRead11 cr11) {
        this.id = cr11.getId();
        this.startTime = cr11.getStartTime().getTime();
        this.endTime = cr11.getEndTime().getTime();
        this.truncationLimit = cr11.getTruncationLimit();
        final TruncationOrder truncationOrder = cr11.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
    }

    @Override
    public List<ComplexRead11Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
