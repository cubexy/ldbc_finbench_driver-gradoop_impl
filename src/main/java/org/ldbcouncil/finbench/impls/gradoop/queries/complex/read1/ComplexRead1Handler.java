package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read1;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead1Handler implements OperationHandler<ComplexRead1, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(ComplexRead1 cr1, GradoopFinbenchBaseGraphState graph,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr1.toString());
        List<ComplexRead1Result> complexRead1Results = new ComplexRead1GradoopOperator(cr1).execute(graph.getGraph());
        resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
    }
}

