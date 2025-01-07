package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read8;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead8Handler implements OperationHandler<ComplexRead8, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead8 cr8, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr8.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead8Result> complexRead8Results =
            new ComplexRead8GradoopOperator(cr8).execute(connectionState.getGraph());
        resultReporter.report(complexRead8Results.size(), complexRead8Results, cr8);
    }
}

