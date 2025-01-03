package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3;

import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead3CmdArgExecutor extends AbstractCmdArgExecutor<ComplexRead3Result> {

    private ComplexRead3GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead3";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_3";
    }

    @Override
    public ComplexRead3Result executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead3 input =
            new ComplexRead3(inputArgs.getId(), inputArgs.getId2(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new ComplexRead3GradoopOperator(input);
    }
}
