package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3;

import java.util.Collections;
import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead3Handler implements OperationHandler<ComplexRead3, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead3 cr3, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr3.toString());
        List<ComplexRead3Result> complexRead3Results =
            Collections.singletonList(new ComplexRead3GradoopOperator(cr3).execute(connectionState.getGraph()));
        resultReporter.report(complexRead3Results.size(), complexRead3Results, cr3);
    }
}

