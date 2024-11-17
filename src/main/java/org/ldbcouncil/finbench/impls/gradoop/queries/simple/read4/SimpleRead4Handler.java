package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead4Handler implements OperationHandler<SimpleRead4, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead4 sr4, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr4.toString());
        List<Tuple3<Long, Integer, Double>> simpleRead4Result = new SimpleRead4GradoopOperator(sr4).execute(connectionState.getGraph());
        List<SimpleRead4Result> simpleRead4Results = new ArrayList<>();
        try {
            simpleRead4Result.forEach(
                tuple -> simpleRead4Results.add(new SimpleRead4Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 4: " + e);
        }
        resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
    }
}

