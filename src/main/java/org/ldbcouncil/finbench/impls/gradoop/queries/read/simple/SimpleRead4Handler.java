package org.ldbcouncil.finbench.impls.gradoop.queries.read.simple;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
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
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.impls.gradoop.GradoopFinbenchBaseGraphState;
import org.ldbcouncil.finbench.impls.gradoop.GradoopImpl;

public class SimpleRead4Handler implements OperationHandler<SimpleRead4, GradoopFinbenchBaseGraphState> {

    @Override
    public void executeOperation(SimpleRead4 sr4, GradoopFinbenchBaseGraphState connectionState,
                                 ResultReporter resultReporter) throws DbException {
        GradoopImpl.logger.info(sr4.toString());
        List<Tuple3<Long, Integer, Double>> simpleRead4Result = new SimpleRead4GradoopOperator(sr4).execute(connectionState.getGraph());
        List<SimpleRead4Result> simpleRead4Results = new ArrayList<>();
        try {
            simpleRead4Result.forEach(
                tuple -> simpleRead4Results.add(new SimpleRead4Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 4: " + e);
        }
        resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
    }
}

class SimpleRead4GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<Tuple3<Long, Integer, Double>>> {

    private final Long id;
    private final Date startTime;
    private final Date endTime;
    private final Double threshold;

    public SimpleRead4GradoopOperator(SimpleRead4 sr4) {
        this.id = sr4.getId();
        this.startTime = sr4.getStartTime();
        this.endTime = sr4.getEndTime();
        this.threshold = sr4.getThreshold();
    }

    @Override
    public List<Tuple3<Long, Integer, Double>> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

        try {
            final long id_serializable = this.id; // this is neccessary because this.id is not serializable, which is needed for the transformVertices function
            TemporalGraph transfers = windowedGraph.query(
                    "MATCH (src:Account)-[transferOut:transfer]->(dst:Account) WHERE src <> dst AND src.id =" +
                        id_serializable +
                        "L AND transferOut.amount > " + this.threshold)
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

            List<Tuple2<TemporalEdge, TemporalVertex>>
                edges = transfers.getEdges().join(transfers.getVertices()).where(new TargetId<>()).equalTo(new Id<>()).collect();

            if (edges.isEmpty()) {
                return Collections.emptyList();
            }

            return edges.stream().map(e -> {
                TemporalEdge edge = e.f0;
                TemporalVertex dst = e.f1;

                long dstId = dst.getPropertyValue("id").getLong(); //error here
                int numEdges = (int) edge.getPropertyValue("count").getLong();
                double sumAmount = roundToDecimalPlaces(edge.getPropertyValue("sum_amount").getDouble(), 3);

                return new Tuple3<>(dstId, numEdges, sumAmount);
                // Sorting is not yet supported in Gradoop, so we have to do it here
            }).sorted(Comparator.comparing((Tuple3<Long, Integer, Double> tuple) -> tuple.f2).reversed()
                .thenComparing(tuple -> tuple.f0)).collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
