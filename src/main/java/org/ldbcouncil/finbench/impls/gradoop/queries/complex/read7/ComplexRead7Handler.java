package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead7Handler implements OperationHandler<ComplexRead7, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead7 cr7, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr7.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead7Result> complexRead7Results =
            new ComplexRead7GradoopOperator(cr7).execute(connectionState.getGraph());
        resultReporter.report(complexRead7Results.size(), complexRead7Results, cr7);
    }
}

