package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple6;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead2Handler implements OperationHandler<SimpleRead2, GradoopFinbenchBaseGraphState> {
    @Override
    public void executeOperation(SimpleRead2 sr2, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr2.toString());
        try {
            Tuple6<Double, Double, Long, Double, Double, Long> sr2Result =
                new SimpleRead2GradoopOperator(sr2).execute(connectionState.getGraph());
            List<SimpleRead2Result> simpleRead2Results = new ArrayList<>();
            simpleRead2Results.add(
                new SimpleRead2Result(sr2Result.f0, sr2Result.f1, sr2Result.f2, sr2Result.f3, sr2Result.f4,
                    sr2Result.f5));
            resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 2: " + e);
        }
    }
}

