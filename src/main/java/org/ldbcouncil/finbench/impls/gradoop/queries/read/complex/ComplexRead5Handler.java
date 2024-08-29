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
import org.ldbcouncil.finbench.driver.result.Path;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead5Handler implements OperationHandler<ComplexRead5, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead5 cr5, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr5.toString());
        DataSet<Tuple1<Path>> complexRead5Result = new ComplexRead5GradoopOperator(cr5).execute(connectionState.getGraph());
        List<ComplexRead5Result> complexRead5Results = new ArrayList<>();
        try {
            complexRead5Result.collect().forEach(
                tuple -> complexRead5Results.add(new ComplexRead5Result(tuple.f0)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 2: " + e);
        }
        resultReporter.report(complexRead5Results.size(), complexRead5Results, cr5);
    }
}

class ComplexRead5GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Path>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Integer truncationLimit;
    private final TruncationOrder truncationOrder;

    public ComplexRead5GradoopOperator(ComplexRead5 cr5) {
        this.id = cr5.getId();
        this.startTime = cr5.getStartTime();
        this.endTime = cr5.getEndTime();
        this.truncationLimit = cr5.getTruncationLimit();
        this.truncationOrder = cr5.getTruncationOrder();
    }

    @Override
    public DataSet<Tuple1<Path>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
