package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead2Handler implements OperationHandler<SimpleRead2, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead2 sr2, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr2.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<SimpleRead2Result> simpleRead2Results =
            new SimpleRead2GradoopOperator(sr2).execute(connectionState.getGraph());
        resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
    }
}

