package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read12;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead12Handler implements OperationHandler<ComplexRead12, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead12 cr12, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr12.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead12Result> complexRead12Results =
            new ComplexRead12GradoopOperator(cr12, connectionState.isFlinkSort()).execute(connectionState.getGraph());
        resultReporter.report(complexRead12Results.size(), complexRead12Results, cr12);
    }
}

