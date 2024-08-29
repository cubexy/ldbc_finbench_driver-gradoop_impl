package org.ldbcouncil.finbench.impls.gradoop.queries.read.simple;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
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
        DataSet<Tuple3<Long, Integer, Double>> simpleRead4Result = new SimpleRead4GradoopOperator(sr4).execute(connectionState.getGraph());
        List<SimpleRead4Result> simpleRead4Results = new ArrayList<>();
        try {
            simpleRead4Result.collect().forEach(
                tuple -> simpleRead4Results.add(new SimpleRead4Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new DbException("Error while collecting results for simple read 4: " + e);
        }
        resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
    }
}

class SimpleRead4GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Integer, Double>>> {

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
    public DataSet<Tuple3<Long, Integer, Double>> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

        try {
            final long id_serializable = this.id; // this is neccessary because this.id is not serializable, which is needed for the transformVertices function
            List<TemporalVertex> accounts = windowedGraph.query(
                    "MATCH (:Account)-[edge2:transfer]->(dst:Account) WHERE src <> dst2 AND src.id =" +
                        id_serializable +
                        "L AND edge2.amount > " + this.threshold)
                .reduce(new ReduceCombination<>())
                .transformVertices((currentVertex, transformedVertex) -> {
                    System.out.println("test");
                    if (currentVertex.hasProperty("id") &&
                        Objects.equals(currentVertex.getPropertyValue("id").getLong(), id_serializable)) {
                        currentVertex.removeProperty("isBlocked");
                    }
                    return currentVertex;
                }).callForGraph(
                    new KeyedGrouping<>(Arrays.asList(GroupingKeys.label(), GroupingKeys.property("isBlocked")),
                        Collections.singletonList(new Count("count")), null,
                        null)
                )
                .getVertices()
                .collect();

            TemporalVertex blockedVertexes = null;
            TemporalVertex nonBlockedVertexes = null;

            for (TemporalVertex vertex : accounts) {
                if (!vertex.hasProperty("isBlocked")) {
                    throw new RuntimeException("List error");
                }
                if (vertex.getPropertyValue("isBlocked").getType() == null) {
                    continue;
                }
                final boolean isBlocked = vertex.getPropertyValue("isBlocked").getBoolean();
                if (isBlocked) {
                    blockedVertexes = vertex;
                } else {
                    nonBlockedVertexes = vertex;
                }
            }

            if (blockedVertexes == null && nonBlockedVertexes == null) {
                return new Tuple1<>(-1.0f);
            }

            if (blockedVertexes == null) {
                return new Tuple1<>(0.0f);
            }

            if (nonBlockedVertexes == null) {
                return new Tuple1<>(1.0f);
            }

            final long blockedAccounts = blockedVertexes.getPropertyValue("count").getLong();
            final long allAccounts = blockedAccounts + nonBlockedVertexes.getPropertyValue("count").getLong();

            return new Tuple1<>(roundToDecimalPlaces((float) blockedAccounts / (float) allAccounts, 3));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
