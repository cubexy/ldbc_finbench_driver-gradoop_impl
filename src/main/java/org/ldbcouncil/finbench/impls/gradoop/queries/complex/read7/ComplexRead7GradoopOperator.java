package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read7;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;

class ComplexRead7GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead7Result>> {

    private final long id;
    private final long startTime;
    private final long endTime;
    private final int truncationLimit;
    private final boolean isTruncationOrderAscending;
    private final double threshold;

    public ComplexRead7GradoopOperator(ComplexRead7 cr7) {
        this.id = cr7.getId();
        this.startTime = cr7.getStartTime().getTime();
        this.endTime = cr7.getEndTime().getTime();
        this.truncationLimit = cr7.getTruncationLimit();
        final TruncationOrder truncationOrder = cr7.getTruncationOrder();
        this.isTruncationOrderAscending = truncationOrder == TruncationOrder.TIMESTAMP_ASCENDING;
        this.threshold = cr7.getThreshold();
    }

    /**
     * Given an Account and a specified time window between startTime and endTime, find all the transfer-
     * in (edge1) and transfer-out (edge2) whose amount exceeds threshold. Return the count of src and
     * dst accounts and the ratio of transfer-in amount over transfer-out amount. The fast-in and fash-out
     * means a tight window between startTime and endTime. Return the ratio as -1 if there is no edge2
     * @param temporalGraph input graph
     * @return list count of src and dst accounts and the ratio of transfer-in amount over transfer-out amount
     */
    @Override
    public List<ComplexRead7Result> execute(TemporalGraph temporalGraph) {
        final long id_serializable = this.id;

        // TODO: implement truncation strategy
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer", "withdraw"))
            .fromTo(this.startTime, this.endTime);

        TemporalGraph tg = windowedGraph.query(
                "MATCH (src:Account)-[edge1:transfer]->(mid:Account)-[edge2:transfer]->(dst:Account) WHERE mid.id = " +
                    this.id + "L AND edge1.amount > " + this.threshold + " AND edge2.amount > " +
                    this.threshold)
            .reduce(new ReduceCombination<>())
            .transformVertices((currentVertex, transformedVertex) -> {
                if (currentVertex.hasProperty("id") &&
                    !Objects.equals(currentVertex.getPropertyValue("id").getLong(), id_serializable)) {
                    currentVertex.removeProperty("id");
                }
                return currentVertex;
            })
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    null, null,
                    Arrays.asList(new Count("count"), new SumProperty("amount"))));

        MapOperator<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<String, Integer, Double>>
            edgeValues = tg.getEdges()
            .join(tg.getVertices()).where(new SourceId<>()).equalTo(new Id<>())
            .map(new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<String, Integer, Double>>() {
                @Override
                public Tuple3<String, Integer, Double> map(Tuple2<TemporalEdge, TemporalVertex> e) {
                    TemporalEdge edge = e.f0;
                    TemporalVertex srcVertex = e.f1;

                    String srcTag = "src";
                    if (srcVertex.getPropertyValue("id").is(Long.class)) {
                        srcTag = "mid";
                    }

                    int edgeCount = (int) edge.getPropertyValue("count").getLong();
                    double edgeSum = edge.getPropertyValue("sum_amount").getDouble();

                    return new Tuple3<>(srcTag, edgeCount, edgeSum);
                }
            });

        List<Tuple3<String, Integer, Double>> edgeValuesList;
        try {
            edgeValuesList = edgeValues.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<ComplexRead7Result> complexRead7Results = new ArrayList<>();

        double edge1Sum = 0.0;
        double edge2Sum = 0.0;
        int edge1Count = 0;
        int edge2Count = 0;

        for (Tuple3<String, Integer, Double> edge : edgeValuesList) {
            if (edge.f0.equals("src")) {
                edge1Count = edge.f1;
                edge1Sum = edge.f2;
                continue;
            }
            if (edge.f0.equals("mid")) {
                edge2Count = edge.f1;
                edge2Sum = edge.f2;
            }
        }

        double inOutRatio = -1.0f;

        if (edge2Sum > 0.0) {
            inOutRatio = roundToDecimalPlaces(edge1Sum / edge2Sum, 3);
        }

        complexRead7Results.add(new ComplexRead7Result(edge1Count, edge2Count, (float) inOutRatio));

        return complexRead7Results;
    }
}
