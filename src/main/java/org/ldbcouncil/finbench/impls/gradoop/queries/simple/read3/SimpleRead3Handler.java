package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple1;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead3Handler implements OperationHandler<SimpleRead3, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead3 sr3, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr3.toString());
        Tuple1<Float> simpleRead3Result = new SimpleRead3GradoopOperator(sr3).execute(connectionState.getGraph());
        List<SimpleRead3Result> simpleRead3Results = new ArrayList<>();
        try {
            simpleRead3Results.add(new SimpleRead3Result(simpleRead3Result.f0));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 3: " + e);
        }
        resultReporter.report(simpleRead3Results.size(), simpleRead3Results, sr3);
    }
}

