package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead4Handler implements OperationHandler<ComplexRead4, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead4 cr4, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr4.toString());
        List<ComplexRead4Result> complexRead4Results =
            new ComplexRead4GradoopOperator(cr4).execute(connectionState.getGraph());
        resultReporter.report(complexRead4Results.size(), complexRead4Results, cr4);
    }
}

