package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;

class ComplexRead7GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Integer, Integer, Float>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;
    private final Double threshold;

    public ComplexRead7GradoopOperator(ComplexRead7 cr7) {
        this.id = cr7.getId();
        this.startTime = cr7.getStartTime();
        this.endTime = cr7.getEndTime();
        this.truncationLimit = cr7.getTruncationLimit();
        this.truncationOrder = cr7.getTruncationOrder();
        this.threshold = cr7.getThreshold();
    }

    @Override
    public DataSet<Tuple3<Integer, Integer, Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
