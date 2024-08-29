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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead10Handler implements OperationHandler<ComplexRead10, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead10 cr10, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr10.toString());
        DataSet<Tuple1<Float>> complexRead10Result = new ComplexRead10GradoopOperator(cr10).execute(connectionState.getGraph());
        List<ComplexRead10Result> complexRead10Results = new ArrayList<>();
        try {
            complexRead10Result.collect().forEach(
                tuple -> complexRead10Results.add(new ComplexRead10Result(tuple.f0)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 10: " + e);
        }
        resultReporter.report(complexRead10Results.size(), complexRead10Results, cr10);
    }
}

class ComplexRead10GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Float>>> {

    private final Long pid1;
    private final Long pid2;
    private final Date startTime;
    private final Date endTime;

    public ComplexRead10GradoopOperator(ComplexRead10 cr10) {
        this.pid1 = cr10.getPid1();
        this.pid2 = cr10.getPid2();
        this.startTime = cr10.getStartTime();
        this.endTime = cr10.getEndTime();
    }

    @Override
    public DataSet<Tuple1<Float>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
