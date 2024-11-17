package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead8Handler implements OperationHandler<ComplexRead8, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead8 cr8, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr8.toString());
        DataSet<Tuple3<Long, Float, Integer>> complexRead8Result = new ComplexRead8GradoopOperator(cr8).execute(connectionState.getGraph());
        List<ComplexRead8Result> complexRead8Results = new ArrayList<>();
        try {
            complexRead8Result.collect().forEach(
                tuple -> complexRead8Results.add(new ComplexRead8Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 8: " + e);
        }
        resultReporter.report(complexRead8Results.size(), complexRead8Results, cr8);
    }
}

