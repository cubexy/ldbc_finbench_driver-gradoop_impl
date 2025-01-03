package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read2;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
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

    /**
     * Given an account, find the sum and max of fund amount in transfer-ins and transfer-outs between
     * them in a specific time range between startTime and endTime. Return the sum and max of amount.
     * For edge1 and edge2, return -1 for the max (maxEdge1Amount and maxEdge2Amount) if there is no
     * transfer.
     * @param temporalGraph input graph
     * @return sum of fund amount in transfer-ins and transfer-outs between them in a specific time range
     */
    @Override
    public List<SimpleRead2Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime()); // Get all transfers between start and end time

        try {
            final Long id = this.id;
            LogicalGraph lg = windowedGraph.query(
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
                ).toLogicalGraph();

            List<Tuple4<Double, Double, Long, String>> edges = lg
                .getEdges()
                .join(lg.getVertices()).where(new SourceId<>()).equalTo(new Id<>())
                .map(new MapFunction<Tuple2<EPGMEdge, EPGMVertex>, Tuple4<Double, Double, Long, String>>() {
                    @Override
                    public Tuple4<Double, Double, Long, String> map(Tuple2<EPGMEdge, EPGMVertex> e) throws Exception {
                        EPGMEdge edge = e.f0;
                        EPGMVertex src = e.f1;

                        double sumAmount = edge.getPropertyValue("sum_amount").getDouble();
                        double maxAmount = edge.getPropertyValue("max_amount").getDouble();
                        long count = edge.getPropertyValue("count").getLong();
                        String type = src.getPropertyValue("id").is(Long.class) ? "transfer-out" : "transfer-in";


                        if (maxAmount == 0.0f) {
                            maxAmount = -1.0f;
                        }

                        return new Tuple4<>(sumAmount, maxAmount, count, type);
                    }
                })
                .collect();

            List<SimpleRead2Result> simpleRead2Results = new ArrayList<>();

            Tuple3<Double, Double, Long> transferIns = new Tuple3<>(0.0, -1.0, 0L); // edges.get(0);
            Tuple3<Double, Double, Long> transferOuts = new Tuple3<>(0.0, -1.0, 0L); // edges.get(1);

            for (Tuple4<Double, Double, Long, String> edge : edges) {
                if (edge.f3.equals("transfer-out")) {
                    transferOuts = new Tuple3<>(edge.f0, edge.f1, edge.f2);
                } else if (edge.f3.equals("transfer-in")) {
                    transferIns = new Tuple3<>(edge.f0, edge.f1, edge.f2);
                }
            }

            simpleRead2Results.add(
                new SimpleRead2Result(roundToDecimalPlaces(transferOuts.f0, 3), roundToDecimalPlaces(transferOuts.f1, 3), transferOuts.f2, roundToDecimalPlaces(transferIns.f0, 3), roundToDecimalPlaces(transferIns.f1, 3),
                    transferIns.f2));

            return simpleRead2Results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
