package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read9;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;

class ComplexRead9GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Float, Float, Float>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;
    private final Double threshold;

    public ComplexRead9GradoopOperator(ComplexRead9 cr9) {
        this.id = cr9.getId();
        this.startTime = cr9.getStartTime();
        this.endTime = cr9.getEndTime();
        this.truncationLimit = cr9.getTruncationLimit();
        this.truncationOrder = cr9.getTruncationOrder();
        this.threshold = cr9.getThreshold();
    }

    @Override
    public DataSet<Tuple3<Float, Float, Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
