package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead3Handler implements OperationHandler<SimpleRead3, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead3 sr3, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr3.toString());
        List<SimpleRead3Result> simpleRead3Results =
            new SimpleRead3GradoopOperator(sr3).execute(connectionState.getGraph());
        resultReporter.report(simpleRead3Results.size(), simpleRead3Results, sr3);
    }
}

