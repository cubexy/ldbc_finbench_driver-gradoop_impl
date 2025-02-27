package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read4;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;

public class SimpleRead4GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead4Result>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Double threshold;
    private final boolean useFlinkSort;

    public SimpleRead4GradoopOperator(SimpleRead4 sr4, boolean useFlinkSort) {
        this.id = sr4.getId();
        this.startTime = sr4.getStartTime();
        this.endTime = sr4.getEndTime();
        this.threshold = sr4.getThreshold();
        this.useFlinkSort = useFlinkSort;
    }

    /**
     * Given an account (src), find all the transfer-outs (edge) from the src to a dst where the amount
     * exceeds threshold in a specific time range between startTime and endTime. Return the count of
     * transfer-outs and the amount sum.
     *
     * @param temporalGraph input graph
     * @return list count of transfer-outs and the amount sum
     */
    @Override
    public List<SimpleRead4Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

        final long id_serializable =
            this.id; // this is necessary because this.id is not serializable, which is needed for the transformVertices function
        TemporalGraph transfers = windowedGraph.query(
                "MATCH (src:Account)-[transferOut:transfer]->(dst:Account) WHERE src <> dst AND src.id =" +
                    id_serializable +
                    "L AND transferOut.amount > " + this.threshold)
            .reduce(new ReduceCombination<>())
            .callForGraph(
                new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                    null, null,
                    Arrays.asList(new Count("count"), new SumProperty("amount")))
            );

        DataSet<Tuple3<Long, Integer, Double>>
            edgeMap = transfers.getEdges().join(transfers.getVertices()).where(new TargetId<>()).equalTo(new Id<>())
            .map(new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<Long, Integer, Double>>() {
                @Override
                public Tuple3<Long, Integer, Double> map(Tuple2<TemporalEdge, TemporalVertex> e) {
                    TemporalEdge edge = e.f0;
                    TemporalVertex src = e.f1;

                    long dstId = src.getPropertyValue("id").getLong();
                    int numEdges = (int) edge.getPropertyValue("count").getLong();
                    double sumAmount = roundToDecimalPlaces(edge.getPropertyValue("sum_amount").getDouble(), 3);

                    return new Tuple3<>(dstId, numEdges, sumAmount);
                }

            });

        if (this.useFlinkSort) {
            windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

            edgeMap = edgeMap
                .sortPartition(2, Order.DESCENDING)
                .sortPartition(0, Order.ASCENDING);
        }

        List<Tuple3<Long, Integer, Double>> edges;
        try {
            edges = edgeMap.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!this.useFlinkSort) {
            edges.sort(Comparator
                .comparing((Tuple3<Long, Integer, Double> t) -> t.f2, Comparator.reverseOrder())
                .thenComparing(t -> t.f0));
        }

        List<SimpleRead4Result> simpleRead4Results = new ArrayList<>();

        for (Tuple3<Long, Integer, Double> edge : edges) {
            simpleRead4Results.add(new SimpleRead4Result(edge.f0, edge.f1, roundToDecimalPlaces(edge.f2, 3)));
        }

        return simpleRead4Results;
    }
}
