package org.ldbcouncil.finbench.impls.gradoop.queries.read.complex;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead4Handler implements OperationHandler<ComplexRead4, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead4 cr4, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr4.toString());
        DataSet<Tuple7<Long, Long, Double, Double, Long, Double, Double>> complexRead4Result = new ComplexRead4GradoopOperator(cr4).execute(connectionState.getGraph());
        List<ComplexRead4Result> complexRead4Results = new ArrayList<>();
        try {
            complexRead4Result.collect().forEach(
                tuple -> complexRead4Results.add(new ComplexRead4Result(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5, tuple.f6)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 4: " + e);
        }
        resultReporter.report(complexRead4Results.size(), complexRead4Results, cr4);
    }
}

class ComplexRead4GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple7<Long, Long, Double, Double, Long, Double, Double>>> {

    private final Date startTime;
    private final Date endTime;
    private final Long id1;
    private final Long id2;

    public ComplexRead4GradoopOperator(ComplexRead4 cr4) {
        this.startTime = cr4.getStartTime();
        this.endTime = cr4.getEndTime();
        this.id1 = cr4.getId1();
        this.id2 = cr4.getId2();
    }

    @Override
    public DataSet<Tuple7<Long, Long, Double, Double, Long, Double, Double>> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
