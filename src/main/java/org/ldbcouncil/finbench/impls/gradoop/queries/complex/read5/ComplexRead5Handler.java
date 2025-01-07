package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read5;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead5Handler implements OperationHandler<ComplexRead5, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead5 cr5, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr5.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead5Result> complexRead5Results =
            new ComplexRead5GradoopOperator(cr5).execute(connectionState.getGraph());
        resultReporter.report(complexRead5Results.size(), complexRead5Results, cr5);
    }
}

