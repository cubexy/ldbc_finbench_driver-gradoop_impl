package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read9;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class ComplexRead9Handler implements OperationHandler<ComplexRead9, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(ComplexRead9 cr9, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr9.toString());
        List<ComplexRead9Result> complexRead9Results = new ComplexRead9GradoopOperator(cr9).execute(connectionState.getGraph());
        resultReporter.report(complexRead9Results.size(), complexRead9Results, cr9);
    }
}

