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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead6Handler implements OperationHandler<ComplexRead6, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead6 cr6, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr6.toString());
        DataSet<Tuple3<Long, Double, Double>> complexRead6Result = new ComplexRead6GradoopOperator(cr6).execute(connectionState.getGraph());
        List<ComplexRead6Result> complexRead6Results = new ArrayList<>();
        try {
            complexRead6Result.collect().forEach(
                tuple -> complexRead6Results.add(new ComplexRead6Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 6: " + e);
        }
        resultReporter.report(complexRead6Results.size(), complexRead6Results, cr6);
    }
}

class ComplexRead6GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Double, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;
    private final Double threshold1;
    private final Double threshold2;

    public ComplexRead6GradoopOperator(ComplexRead6 cr6) {
        this.id = cr6.getId();
        this.startTime = cr6.getStartTime();
        this.endTime = cr6.getEndTime();
        this.truncationLimit = cr6.getTruncationLimit();
        this.truncationOrder = cr6.getTruncationOrder();
        this.threshold1 = cr6.getThreshold1();
        this.threshold2 = cr6.getThreshold2();
    }

    @Override
    public DataSet<Tuple3<Long, Double, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
