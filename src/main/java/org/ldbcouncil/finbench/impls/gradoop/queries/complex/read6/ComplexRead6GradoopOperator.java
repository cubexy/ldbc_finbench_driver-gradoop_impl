package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;

class ComplexRead6GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Double, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;
    private final Double threshold1;
    private final Double threshold2;

    public ComplexRead6GradoopOperator(ComplexRead6 cr6) {
        this.id = cr6.getId();
        this.startTime = cr6.getStartTime();
        this.endTime = cr6.getEndTime();
        this.truncationLimit = cr6.getTruncationLimit();
        this.truncationOrder = cr6.getTruncationOrder();
        this.threshold1 = cr6.getThreshold1();
        this.threshold2 = cr6.getThreshold2();
    }

    @Override
    public DataSet<Tuple3<Long, Double, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
