package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead11Handler implements OperationHandler<ComplexRead11, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead11 cr11, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr11.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead11Result> complexRead11Results =
            new ComplexRead11GradoopOperator(cr11).execute(connectionState.getGraph());
        resultReporter.report(complexRead11Results.size(), complexRead11Results, cr11);
    }
}

