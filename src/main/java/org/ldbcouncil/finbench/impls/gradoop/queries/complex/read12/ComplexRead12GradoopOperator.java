package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;

class ComplexRead12GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple2<Long, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead12GradoopOperator(ComplexRead12 cr12) {
        this.id = cr12.getId();
        this.startTime = cr12.getStartTime();
        this.endTime = cr12.getEndTime();
        this.truncationLimit = cr12.getTruncationLimit();
        this.truncationOrder = cr12.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple2<Long, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
