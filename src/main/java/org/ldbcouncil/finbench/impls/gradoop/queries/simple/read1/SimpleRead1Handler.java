package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

import java.util.List;

public class SimpleRead1Handler implements OperationHandler<SimpleRead1, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead1 sr1, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr1.toString());
        List<SimpleRead1Result> simpleRead1Results = new SimpleRead1GradoopOperator(sr1).execute(connectionState.getGraph());
        resultReporter.report(simpleRead1Results.size(), simpleRead1Results, sr1);
    }
}

