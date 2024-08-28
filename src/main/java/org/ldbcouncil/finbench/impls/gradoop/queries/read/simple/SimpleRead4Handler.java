package org.ldbcouncil.finbench.impls.gradoop.queries.read.simple;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead4Handler implements OperationHandler<SimpleRead4, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead4 sr4, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr4.toString());
        DataSet<Tuple3<Long, Integer, Double>> simpleRead4Result = new SimpleRead4GradoopOperator(sr4).execute(connectionState.getGraph());
        List<SimpleRead4Result> simpleRead4Results = new ArrayList<>();
        try {
            simpleRead4Result.collect().forEach(
                tuple -> simpleRead4Results.add(new SimpleRead4Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 1: " + e);
        }
        resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
    }
}

class SimpleRead4GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Integer, Double>>> {

    private final long id;
    private final Date startTime;
    private final Date endTime;
    private final double threshold;

    public SimpleRead4GradoopOperator(SimpleRead4 sr4) {
        this.id = sr4.getId();
        this.startTime = sr4.getStartTime();
        this.endTime = sr4.getEndTime();
        this.threshold = sr4.getThreshold();
    }

    @Override
    public DataSet<Tuple3<Long, Integer, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
