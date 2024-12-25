package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read4;

import java.util.Arrays;
import java.util.List;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
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
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime, this.endTime);

        windowedGraph.query(
            "MATCH (src:Account)-[edge1:transfer]->(dst:Account), (src)<-[edge2:transfer]-(other:Account)-[edge3:transfer]->(dst) WHERE src.id = " +
                this.id1 + "L AND dst.id = " + this.id2 + "L")
            .reduce(new ReduceCombination<>())
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    null, null,
                    Arrays.asList(new Count("count"), new SumProperty("amount")))
            );
        //group by other.id, remove other ids (src and dst), aggregate by count and sum(amount)



        return null;
    }
}
