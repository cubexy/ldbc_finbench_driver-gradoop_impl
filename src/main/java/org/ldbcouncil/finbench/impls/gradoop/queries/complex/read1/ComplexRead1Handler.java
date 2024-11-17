package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead1Handler implements OperationHandler<ComplexRead1, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(ComplexRead1 cr1, GradoopFinbenchBaseGraphState graph,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr1.toString());
        ComplexRead1GradoopOperator complexRead1GradoopOperator = new ComplexRead1GradoopOperator(cr1);
        DataSet<Tuple4<Long, Integer, Long, String>> cr1Result = complexRead1GradoopOperator.execute(graph.getGraph());
        List<ComplexRead1Result> complexRead1Results = new ArrayList<>();
        try {
            cr1Result.collect().forEach(
                tuple -> complexRead1Results.add(new ComplexRead1Result(tuple.f0, tuple.f1, tuple.f2, tuple.f3)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 1: " + e);
        }
        resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
    }
}

