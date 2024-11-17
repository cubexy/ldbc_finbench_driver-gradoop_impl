package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
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

