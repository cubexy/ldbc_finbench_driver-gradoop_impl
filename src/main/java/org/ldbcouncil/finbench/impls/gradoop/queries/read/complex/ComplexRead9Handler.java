package org.ldbcouncil.finbench.impls.gradoop.queries.read.complex;

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
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead9Handler implements OperationHandler<ComplexRead9, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead9 cr9, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr9.toString());
        DataSet<Tuple3<Float, Float, Float>> complexRead9Result = new ComplexRead9GradoopOperator(cr9).execute(connectionState.getGraph());
        List<ComplexRead9Result> complexRead9Results = new ArrayList<>();
        try {
            complexRead9Result.collect().forEach(
                tuple -> complexRead9Results.add(new ComplexRead9Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 9: " + e);
        }
        resultReporter.report(complexRead9Results.size(), complexRead9Results, cr9);
    }
}

class ComplexRead9GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Float, Float, Float>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;
    private final Double threshold;

    public ComplexRead9GradoopOperator(ComplexRead9 cr9) {
        this.id = cr9.getId();
        this.startTime = cr9.getStartTime();
        this.endTime = cr9.getEndTime();
        this.truncationLimit = cr9.getTruncationLimit();
        this.truncationOrder = cr9.getTruncationOrder();
        this.threshold = cr9.getThreshold();
    }

    @Override
    public DataSet<Tuple3<Float, Float, Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
