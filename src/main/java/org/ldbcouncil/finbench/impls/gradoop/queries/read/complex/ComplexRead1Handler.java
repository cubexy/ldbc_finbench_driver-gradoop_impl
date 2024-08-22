package org.ldbcouncil.finbench.impls.gradoop.queries.read.complex;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.GradoopConnectionState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

import java.util.ArrayList;
import java.util.List;

public class ComplexRead1Handler implements OperationHandler<ComplexRead1, GradoopConnectionState> {
    @Override
    public void executeOperation(ComplexRead1 cr1, GradoopConnectionState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(cr1.toString());

        //The output of ComplexReads is the input of SimpleReads,
        // so ComplexRead1 outputs some results for verify that SimpleReads are correct.
        List<ComplexRead1Result> complexRead1Results = new ArrayList<>();
        complexRead1Results.add(new ComplexRead1Result(1, 0, 0, "101"));
        complexRead1Results.add(new ComplexRead1Result(2, 0, 0, "102"));
        complexRead1Results.add(new ComplexRead1Result(3, 0, 0, "103"));
        complexRead1Results.add(new ComplexRead1Result(4, 0, 0, "104"));
        complexRead1Results.add(new ComplexRead1Result(5, 0, 0, "105"));
        complexRead1Results.add(new ComplexRead1Result(6, 0, 0, "106"));
        complexRead1Results.add(new ComplexRead1Result(7, 0, 0, "107"));
        complexRead1Results.add(new ComplexRead1Result(8, 0, 0, "108"));
        complexRead1Results.add(new ComplexRead1Result(9, 0, 0, "109"));
        complexRead1Results.add(new ComplexRead1Result(10, 0, 0, "1010"));
        complexRead1Results.add(new ComplexRead1Result(11, 0, 0, "1011"));
        complexRead1Results.add(new ComplexRead1Result(12, 0, 0, "1012"));
        complexRead1Results.add(new ComplexRead1Result(13, 0, 0, "1013"));
        complexRead1Results.add(new ComplexRead1Result(14, 0, 0, "1014"));
        complexRead1Results.add(new ComplexRead1Result(15, 0, 0, "1015"));
        complexRead1Results.add(new ComplexRead1Result(16, 0, 0, "1016"));
        complexRead1Results.add(new ComplexRead1Result(17, 0, 0, "1017"));
        complexRead1Results.add(new ComplexRead1Result(18, 0, 0, "1018"));
        resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
    }
}