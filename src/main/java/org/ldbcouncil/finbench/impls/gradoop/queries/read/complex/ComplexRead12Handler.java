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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead12Handler implements OperationHandler<ComplexRead12, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead12 cr12, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr12.toString());
        DataSet<Tuple2<Long, Double>> complexRead12Result = new ComplexRead12GradoopOperator(cr12).execute(connectionState.getGraph());
        List<ComplexRead12Result> complexRead12Results = new ArrayList<>();
        try {
            complexRead12Result.collect().forEach(
                tuple -> complexRead12Results.add(new ComplexRead12Result(tuple.f0, tuple.f1)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 12: " + e);
        }
        resultReporter.report(complexRead12Results.size(), complexRead12Results, cr12);
    }
}

class ComplexRead12GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple2<Long, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead12GradoopOperator(ComplexRead12 cr12) {
        this.id = cr12.getId();
        this.startTime = cr12.getStartTime();
        this.endTime = cr12.getEndTime();
        this.truncationLimit = cr12.getTruncationLimit();
        this.truncationOrder = cr12.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple2<Long, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
