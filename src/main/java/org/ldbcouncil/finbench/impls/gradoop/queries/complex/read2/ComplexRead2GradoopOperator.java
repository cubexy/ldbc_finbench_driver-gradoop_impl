package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;

class ComplexRead2GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Double, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead2GradoopOperator(ComplexRead2 cr2) {
        this.id = cr2.getId();
        this.startTime = cr2.getStartTime();
        this.endTime = cr2.getEndTime();
        this.truncationLimit = cr2.getTruncationLimit();
        this.truncationOrder = cr2.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple3<Long, Double, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
