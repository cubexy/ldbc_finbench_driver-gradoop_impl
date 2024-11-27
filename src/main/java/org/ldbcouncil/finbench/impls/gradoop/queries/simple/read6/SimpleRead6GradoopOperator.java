package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read6;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;

class SimpleRead6GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Long>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead6GradoopOperator(SimpleRead6 sr6) {
        this.id = sr6.getId();
        this.startTime = sr6.getStartTime();
        this.endTime = sr6.getEndTime();
    }

    @Override
    public DataSet<Tuple1<Long>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
