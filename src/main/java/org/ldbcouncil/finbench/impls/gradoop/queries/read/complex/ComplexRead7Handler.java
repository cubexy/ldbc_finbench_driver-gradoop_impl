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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead7Handler implements OperationHandler<ComplexRead7, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead7 cr7, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr7.toString());
        DataSet<Tuple3<Integer, Integer, Float>> complexRead7Result = new ComplexRead7GradoopOperator(cr7).execute(connectionState.getGraph());
        List<ComplexRead7Result> complexRead7Results = new ArrayList<>();
        try {
            complexRead7Result.collect().forEach(
                tuple -> complexRead7Results.add(new ComplexRead7Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 7: " + e);
        }
        resultReporter.report(complexRead7Results.size(), complexRead7Results, cr7);
    }
}

class ComplexRead7GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Integer, Integer, Float>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;
    private final Double threshold;

    public ComplexRead7GradoopOperator(ComplexRead7 cr7) {
        this.id = cr7.getId();
        this.startTime = cr7.getStartTime();
        this.endTime = cr7.getEndTime();
        this.truncationLimit = cr7.getTruncationLimit();
        this.truncationOrder = cr7.getTruncationOrder();
        this.threshold = cr7.getThreshold();
    }

    @Override
    public DataSet<Tuple3<Integer, Integer, Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
