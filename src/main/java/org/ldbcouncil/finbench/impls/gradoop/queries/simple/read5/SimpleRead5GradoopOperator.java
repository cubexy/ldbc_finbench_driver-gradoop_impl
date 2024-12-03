package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read5;

import java.util.Date;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;

class SimpleRead5GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Integer, Float>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Double threshold;

    public SimpleRead5GradoopOperator(SimpleRead5 sr5) {
        this.id = sr5.getId();
        this.threshold = sr5.getThreshold();
        this.startTime = sr5.getStartTime();
        this.endTime = sr5.getEndTime();
    }

    @Override
    public DataSet<Tuple3<Long, Integer, Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
