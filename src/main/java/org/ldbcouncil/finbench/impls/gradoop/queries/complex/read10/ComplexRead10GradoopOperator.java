package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read10;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;

class ComplexRead10GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Float>>> {

    private final Long pid1;
    private final Long pid2;
    private final Date startTime;
    private final Date endTime;

    public ComplexRead10GradoopOperator(ComplexRead10 cr10) {
        this.pid1 = cr10.getPid1();
        this.pid2 = cr10.getPid2();
        this.startTime = cr10.getStartTime();
        this.endTime = cr10.getEndTime();
    }

    @Override
    public DataSet<Tuple1<Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
