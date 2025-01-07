package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read10;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead10Handler implements OperationHandler<ComplexRead10, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead10 cr10, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr10.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead10Result> complexRead10Results =
            new ComplexRead10GradoopOperator(cr10).execute(connectionState.getGraph());
        resultReporter.report(complexRead10Results.size(), complexRead10Results, cr10);
    }
}

