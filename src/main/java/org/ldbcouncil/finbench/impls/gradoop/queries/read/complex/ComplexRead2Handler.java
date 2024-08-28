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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead2Handler implements OperationHandler<ComplexRead2, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead2 cr2, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr2.toString());
        DataSet<Tuple3<Long, Double, Double>> complexRead2Result = new ComplexRead2GradoopOperator(cr2).execute(connectionState.getGraph());
        List<ComplexRead2Result> complexRead2Results = new ArrayList<>();
        try {
            complexRead2Result.collect().forEach(
                tuple -> complexRead2Results.add(new ComplexRead2Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 2: " + e);
        }
        resultReporter.report(complexRead2Results.size(), complexRead2Results, cr2);
    }
}

class ComplexRead2GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Double, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead2GradoopOperator(ComplexRead2 cr2) {
        this.id = cr2.getId();
        this.startTime = cr2.getStartTime();
        this.endTime = cr2.getEndTime();
        this.truncationLimit = cr2.getTruncationLimit();
        this.truncationOrder = cr2.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple3<Long, Double, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
