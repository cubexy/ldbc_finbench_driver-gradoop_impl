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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead5Handler implements OperationHandler<SimpleRead5, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead5 sr5, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr5.toString());
        DataSet<Tuple3<Long, Integer, Float>> simpleRead5Result = new SimpleRead5GradoopOperator(sr5).execute(connectionState.getGraph());
        List<SimpleRead5Result> simpleRead5Results = new ArrayList<>();
        try {
            simpleRead5Result.collect().forEach(
                tuple -> simpleRead5Results.add(new SimpleRead5Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 5: " + e);
        }
        resultReporter.report(simpleRead5Results.size(), simpleRead5Results, sr5);
    }
}

class SimpleRead5GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Integer, Float>>> {

    private final long id;
    private final Date startTime;
    private final Date endTime;
    private final double threshold;

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
