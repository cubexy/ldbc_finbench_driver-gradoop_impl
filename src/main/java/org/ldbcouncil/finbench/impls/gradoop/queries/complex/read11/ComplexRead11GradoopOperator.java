package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;

class ComplexRead11GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple2<Double, Integer>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead11GradoopOperator(ComplexRead11 cr11) {
        this.id = cr11.getId();
        this.startTime = cr11.getStartTime();
        this.endTime = cr11.getEndTime();
        this.truncationLimit = cr11.getTruncationLimit();
        this.truncationOrder = cr11.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple2<Double, Integer>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
