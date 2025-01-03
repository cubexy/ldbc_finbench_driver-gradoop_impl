package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read11;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead11CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead11Result>> {

    private ComplexRead11GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead11";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_11";
    }

    @Override
    public List<ComplexRead11Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead11 input = new ComplexRead11(inputArgs.getId(), inputArgs.getStartTime(), inputArgs.getEndTime(),
            inputArgs.getTruncationLimit(), inputArgs.getTruncationOrder());
        this.operator = new ComplexRead11GradoopOperator(input);
    }
}
