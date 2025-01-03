package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import java.util.List;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;
import org.ldbcouncil.finbench.impls.gradoop.FlinkCmdArg;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.queries.AbstractCmdArgExecutor;

public class ComplexRead4CmdArgExecutor extends AbstractCmdArgExecutor<List<ComplexRead4Result>> {

    private ComplexRead4GradoopOperator operator;

    @Override
    public String getQueryTitle() {
        return "ComplexRead4";
    }

    @Override
    public String getQueryKey() {
        return "complex_read_4";
    }

    @Override
    public List<ComplexRead4Result> executeQuery(GradoopFinbenchBaseGraphState graph) throws DbException {
        return operator.execute(graph.getGraph());
    }

    @Override
    public void init(FlinkCmdArg inputArgs) {
        ComplexRead4 input =
            new ComplexRead4(inputArgs.getId(), inputArgs.getId2(), inputArgs.getStartTime(), inputArgs.getEndTime());
        this.operator = new ComplexRead4GradoopOperator(input);
    }
}
