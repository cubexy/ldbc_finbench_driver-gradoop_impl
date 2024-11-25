package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read6;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead6Handler implements OperationHandler<SimpleRead6, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead6 sr6, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr6.toString());
        DataSet<Tuple1<Long>> simpleRead6Result = new SimpleRead6GradoopOperator(sr6).execute(connectionState.getGraph());
        List<SimpleRead6Result> simpleRead6Results = new ArrayList<>();
        try {
            simpleRead6Result.collect().forEach(
                tuple -> simpleRead6Results.add(new SimpleRead6Result(tuple.f0)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 6: " + e);
        }
        resultReporter.report(simpleRead6Results.size(), simpleRead6Results, sr6);
    }
}
