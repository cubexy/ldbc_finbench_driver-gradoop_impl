package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;

class ComplexRead4GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead4Result>> {

    private final long startTime;
    private final long endTime;
    private final long id1;
    private final long id2;

    public ComplexRead4GradoopOperator(ComplexRead4 cr4) {
        this.startTime = cr4.getStartTime().getTime();
        this.endTime = cr4.getEndTime().getTime();
        this.id1 = cr4.getId1();
        this.id2 = cr4.getId2();
    }

    @Override
    public List<ComplexRead4Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
