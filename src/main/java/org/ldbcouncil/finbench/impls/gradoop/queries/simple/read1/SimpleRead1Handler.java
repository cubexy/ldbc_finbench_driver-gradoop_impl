package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SimpleRead1Handler implements OperationHandler<SimpleRead1, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead1 sr1, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr1.toString());
        DataSet<Tuple3<Date, Boolean, String>> simpleRead1Result = new SimpleRead1GradoopOperator(sr1).execute(connectionState.getGraph());
        List<SimpleRead1Result> simpleRead1Results = new ArrayList<>();
        try {
            simpleRead1Result.collect().forEach(
                tuple -> simpleRead1Results.add(new SimpleRead1Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 1: " + e);
        }
        resultReporter.report(simpleRead1Results.size(), simpleRead1Results, sr1);
    }
}

