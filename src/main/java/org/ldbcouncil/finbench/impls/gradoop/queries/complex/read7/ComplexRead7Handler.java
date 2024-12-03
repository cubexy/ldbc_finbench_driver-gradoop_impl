package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;
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

