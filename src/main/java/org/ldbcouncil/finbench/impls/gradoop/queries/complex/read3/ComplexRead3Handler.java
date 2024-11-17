package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
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
        DataSet<Tuple1<Long>> complexRead3Result = new ComplexRead3GradoopOperator(cr3).execute(connectionState.getGraph());
        List<ComplexRead3Result> complexRead3Results = new ArrayList<>();
        try {
            complexRead3Result.collect().forEach(
                tuple -> complexRead3Results.add(new ComplexRead3Result(tuple.f0)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for complex read 3: " + e);
        }
        resultReporter.report(complexRead3Results.size(), complexRead3Results, cr3);
    }
}

