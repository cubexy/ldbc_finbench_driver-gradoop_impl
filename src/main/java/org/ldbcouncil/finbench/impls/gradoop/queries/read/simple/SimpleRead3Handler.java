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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead3Handler implements OperationHandler<SimpleRead3, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead3 sr3, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr3.toString());
        DataSet<Tuple1<Float>> simpleRead3Result = new SimpleRead3GradoopOperator(sr3).execute(connectionState.getGraph());
        List<SimpleRead3Result> simpleRead3Results = new ArrayList<>();
        try {
            simpleRead3Result.collect().forEach(
                tuple -> simpleRead3Results.add(new SimpleRead3Result(tuple.f0)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 3: " + e);
        }
        resultReporter.report(simpleRead3Results.size(), simpleRead3Results, sr3);
    }
}

class SimpleRead3GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Float>>> {

    private final long id;
    private final double threshold;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead3GradoopOperator(SimpleRead3 sr3) {
        this.id = sr3.getId();
        this.threshold = sr3.getThreshold();
        this.startTime = sr3.getStartTime();
        this.endTime = sr3.getEndTime();
    }

    @Override
    public DataSet<Tuple1<Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
