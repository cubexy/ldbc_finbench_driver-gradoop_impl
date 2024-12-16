package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;

public class SimpleRead3GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead3Result>> {

    private final Long id;
    private final Double threshold;
    private final Date startTime;
    private final Date endTime;

    public SimpleRead3GradoopOperator(SimpleRead3 sr3) {
        this.id = sr3.getId();
        this.threshold = sr3.getThreshold();
        this.startTime = sr3.getStartTime();
        this.endTime = sr3.getEndTime();
    }

    @Override
    public List<SimpleRead3Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

        try {
            final long id_serializable =
                this.id; // this is necessary because this.id is not serializable, which is needed for the transformVertices function
            List<TemporalVertex> accounts = windowedGraph.query(
                    "MATCH (src:Account)-[transferIn:transfer]->(dst:Account) WHERE src <> person AND dst.id =" +
                        id_serializable +
                        "L AND transferIn.amount > " + this.threshold)
                .reduce(new ReduceCombination<>())
                .transformVertices((currentVertex, transformedVertex) -> {
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

            List<SimpleRead3Result> simpleRead3Results = new ArrayList<>();
            if (blockedVertexes == null && nonBlockedVertexes == null) {
                simpleRead3Results.add(new SimpleRead3Result(-1.0f));
                return simpleRead3Results;
            }

            if (blockedVertexes == null) {
                simpleRead3Results.add(new SimpleRead3Result(-0.0f));
                return simpleRead3Results;
            }

            if (nonBlockedVertexes == null) {
                simpleRead3Results.add(new SimpleRead3Result(1.0f));
                return simpleRead3Results;
            }

            final long blockedAccounts = blockedVertexes.getPropertyValue("count").getLong();
            final long allAccounts = blockedAccounts + nonBlockedVertexes.getPropertyValue("count").getLong();

            simpleRead3Results.add(new SimpleRead3Result(roundToDecimalPlaces((float) blockedAccounts / (float) allAccounts, 3)));
            return simpleRead3Results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
