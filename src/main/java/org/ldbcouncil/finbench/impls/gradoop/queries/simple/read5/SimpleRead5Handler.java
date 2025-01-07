package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read5;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead5Handler implements OperationHandler<SimpleRead5, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead5 sr5, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr5.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<SimpleRead5Result> simpleRead5Results =
            new SimpleRead5GradoopOperator(sr5, connectionState.isFlinkSort()).execute(connectionState.getGraph());
        resultReporter.report(simpleRead5Results.size(), simpleRead5Results, sr5);
    }
}

