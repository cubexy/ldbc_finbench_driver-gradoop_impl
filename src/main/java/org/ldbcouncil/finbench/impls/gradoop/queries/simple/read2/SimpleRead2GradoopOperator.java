package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;

public class SimpleRead2GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead2Result>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead2GradoopOperator(SimpleRead2 sr2) {
        this.id = sr2.getId();
        this.startTime = sr2.getStartTime();
        this.endTime = sr2.getEndTime();
    }

    @Override
    public List<SimpleRead2Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime()); // Get all transfers between start and end time

        try {
            final Long id = this.id;
            List<Tuple3<Double, Double, Long>> edges = windowedGraph.query(
                    "MATCH (src:Account)-[edge1:transfer]->(dst1:Account) WHERE src <> dst1 AND src.id =" + this.id + "L")
                .union(windowedGraph.query(
                    "MATCH (dst2:Account)-[edge2:transfer]->(src:Account) WHERE src <> dst2 AND src.id =" + this.id +
                        "L"))
                .reduce(new ReduceCombination<>())
                .transformVertices((currentVertex, transformedVertex) -> {
                    if (currentVertex.hasProperty("id") &&
                        !Objects.equals(currentVertex.getPropertyValue("id").getLong(), id)) {
                        currentVertex.removeProperty("id");
                    }
                    return currentVertex;
                }).callForGraph(
                    new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")), null, null,
                        Arrays.asList(new Count("count"), new SumProperty("amount"), new MaxProperty("amount")))
                )
                .toLogicalGraph()
                .getEdges()
                .map(new MapFunction<EPGMEdge, Tuple3<Double, Double, Long>>() {
                    @Override
                    public Tuple3<Double, Double, Long> map(EPGMEdge edge) throws Exception {
                        double sumAmount = edge.getPropertyValue("sum_amount").getDouble();
                        double maxAmount = edge.getPropertyValue("max_amount").getDouble();
                        long count = edge.getPropertyValue("count").getLong();

                        if (maxAmount == 0.0f) {
                            maxAmount = -1.0f;
                        }

                        return new Tuple3<>(sumAmount, maxAmount, count);
                    }
                })
                .collect();

            final Tuple3<Double, Double, Long> transferIns = edges.get(0);
            final Tuple3<Double, Double, Long> transferOuts = edges.get(1);


            List<SimpleRead2Result> simpleRead2Results = new ArrayList<>();
            simpleRead2Results.add(
                new SimpleRead2Result(roundToDecimalPlaces(transferOuts.f0, 3), roundToDecimalPlaces(transferOuts.f1, 3), transferOuts.f2, roundToDecimalPlaces(transferIns.f0, 3), roundToDecimalPlaces(transferIns.f1, 3),
                    transferIns.f2));

            return simpleRead2Results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
