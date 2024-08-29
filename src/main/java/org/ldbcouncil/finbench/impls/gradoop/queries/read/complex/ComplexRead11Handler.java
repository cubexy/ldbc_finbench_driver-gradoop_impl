package org.ldbcouncil.finbench.impls.gradoop.queries.read.complex;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead11Handler implements OperationHandler<ComplexRead11, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead11 cr11, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr11.toString());
        DataSet<Tuple2<Double, Integer>> complexRead11Result = new ComplexRead11GradoopOperator(cr11).execute(connectionState.getGraph());
        List<ComplexRead11Result> complexRead11Results = new ArrayList<>();
        try {
            complexRead11Result.collect().forEach(
                tuple -> complexRead11Results.add(new ComplexRead11Result(tuple.f0, tuple.f1)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 11: " + e);
        }
        resultReporter.report(complexRead11Results.size(), complexRead11Results, cr11);
    }
}

class ComplexRead11GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple2<Double, Integer>>> {

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
