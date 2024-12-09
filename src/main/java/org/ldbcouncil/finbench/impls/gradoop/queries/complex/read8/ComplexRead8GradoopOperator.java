package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;

class ComplexRead8GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Float, Integer>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final Float threshold;
    private final TruncationOrder truncationOrder;

    public ComplexRead8GradoopOperator(ComplexRead8 cr8) {
        this.id = cr8.getId();
        this.startTime = cr8.getStartTime();
        this.endTime = cr8.getEndTime();
        this.truncationLimit = cr8.getTruncationLimit();
        this.truncationOrder = cr8.getTruncationOrder();
        this.threshold = cr8.getThreshold();
    }

    @Override
    public DataSet<Tuple3<Long, Float, Integer>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
