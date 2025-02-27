package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read2;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead2Handler implements OperationHandler<ComplexRead2, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead2 cr2, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr2.toString());
        CommonUtils.setInitialParallelism(connectionState);
        List<ComplexRead2Result> complexRead2Results =
            new ComplexRead2GradoopOperator(cr2, connectionState.isFlinkSort()).execute(connectionState.getGraph());
        resultReporter.report(complexRead2Results.size(), complexRead2Results, cr2);
    }
}

