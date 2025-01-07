package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead4Handler implements OperationHandler<SimpleRead4, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead4 sr4, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr4.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<SimpleRead4Result> simpleRead4Results =
            new SimpleRead4GradoopOperator(sr4, connectionState.getParallelism(), connectionState.isFlinkSort()).execute(connectionState.getGraph());
        resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
    }
}

