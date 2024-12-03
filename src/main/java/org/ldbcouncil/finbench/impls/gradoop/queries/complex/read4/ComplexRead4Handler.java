package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple7;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;
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

