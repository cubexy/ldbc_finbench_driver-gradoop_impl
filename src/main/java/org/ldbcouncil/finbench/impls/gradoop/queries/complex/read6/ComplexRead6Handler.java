package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read6;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead6Handler implements OperationHandler<ComplexRead6, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead6 cr6, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr6.toString());
        List<ComplexRead6Result> complexRead6Results =
            new ComplexRead6GradoopOperator(cr6).execute(connectionState.getGraph());
        resultReporter.report(complexRead6Results.size(), complexRead6Results, cr6);
    }
}

