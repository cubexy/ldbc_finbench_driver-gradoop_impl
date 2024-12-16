package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read5;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.operators.MapOperator;
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
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5Result;

class SimpleRead5GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead5Result>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Double threshold;

    public SimpleRead5GradoopOperator(SimpleRead5 sr5) {
        this.id = sr5.getId();
        this.threshold = sr5.getThreshold();
        this.startTime = sr5.getStartTime();
        this.endTime = sr5.getEndTime();
    }

    @Override
    public List<SimpleRead5Result> execute(TemporalGraph temporalGraph) {

        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

        try {
            final long id_serializable =
                this.id; // this is necessary because this.id is not serializable, which is needed for the transformVertices function
            TemporalGraph transfers = windowedGraph.query(
                    "MATCH (dst:Account)<-[transferIn:transfer]-(src:Account) WHERE src <> dst AND dst.id =" +
                        id_serializable +
                        "L AND transferIn.amount > " + this.threshold)
                .reduce(new ReduceCombination<>())
                .transformVertices((currentVertex, transformedVertex) -> {
                    if (currentVertex.hasProperty("id") &&
                        Objects.equals(currentVertex.getPropertyValue("id").getLong(), id_serializable)) {
                        currentVertex.removeProperty("id");
                    }
                    return currentVertex;
                }).callForGraph(
                    new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("id")),
                        null, null,
                        Arrays.asList(new Count("count"), new SumProperty("amount")))
                );

            MapOperator<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<Long, Integer, Double>>
                edgeMap = transfers.getEdges().join(transfers.getVertices()).where(new TargetId<>()).equalTo(new Id<>())
                .map(new MapFunction<Tuple2<TemporalEdge, TemporalVertex>, Tuple3<Long, Integer, Double>>() {
                    @Override
                    public Tuple3<Long, Integer, Double> map(Tuple2<TemporalEdge, TemporalVertex> e) throws Exception {
                        TemporalEdge edge = e.f0;
                        TemporalVertex dst = e.f1;

                        long dstId = dst.getPropertyValue("id").getLong(); //error here
                        int numEdges = (int) edge.getPropertyValue("count").getLong();
                        double sumAmount = roundToDecimalPlaces(edge.getPropertyValue("sum_amount").getDouble(), 3);

                        return new Tuple3<>(dstId, numEdges, sumAmount);
                    }

                });

            windowedGraph.getConfig().getExecutionEnvironment().setParallelism(1);

            List<Tuple3<Long, Integer, Double>> edges = edgeMap
                .sortPartition(2, Order.DESCENDING)
                .sortPartition(0, Order.ASCENDING)
                .collect();

            List<SimpleRead5Result> simpleRead5Results = new ArrayList<>();

            for (Tuple3<Long, Integer, Double> edge : edges) {
                simpleRead5Results.add(new SimpleRead5Result(edge.f0, edge.f1, roundToDecimalPlaces(edge.f2, 3)));
            }

            return simpleRead5Results;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
