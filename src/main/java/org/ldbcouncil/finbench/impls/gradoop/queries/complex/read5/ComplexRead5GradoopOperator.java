package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read5;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.result.Path;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;

class ComplexRead5GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Path>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead5GradoopOperator(ComplexRead5 cr5) {
        this.id = cr5.getId();
        this.startTime = cr5.getStartTime();
        this.endTime = cr5.getEndTime();
        this.truncationLimit = cr5.getTruncationLimit();
        this.truncationOrder = cr5.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple1<Path>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
