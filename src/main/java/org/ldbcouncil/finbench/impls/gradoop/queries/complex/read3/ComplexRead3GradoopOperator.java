package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read3;

import java.util.Date;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;

class ComplexRead3GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead3Result>> {

    private final Date startTime;
    private final Date endTime;
    private final Long id1;
    private final Long id2;

    public ComplexRead3GradoopOperator(ComplexRead3 cr3) {
        this.startTime = cr3.getStartTime();
        this.endTime = cr3.getEndTime();
        this.id1 = cr3.getId1();
        this.id2 = cr3.getId2();
    }

    @Override
    public List<ComplexRead3Result> execute(TemporalGraph temporalGraph) {
        return null;
    }
}
