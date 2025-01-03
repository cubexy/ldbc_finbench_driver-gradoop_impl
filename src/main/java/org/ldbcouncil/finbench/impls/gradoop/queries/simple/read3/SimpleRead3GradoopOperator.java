package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class SimpleRead3GradoopOperator
    implements UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead3Result>> {

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


        final long id_serializable =
            this.id; // this is necessary because this.id is not serializable, which is needed for the transformVertices function
        DataSet<Tuple2<Integer, Integer>> accounts = windowedGraph.query(
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
            .map(new MapFunction<TemporalVertex, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> map(TemporalVertex temporalVertex) throws Exception {
                    if (temporalVertex.getPropertyValue("isBlocked").getType() == null) { // dst account
                        return new Tuple2<>(0, 0);
                    }
                    final boolean isBlocked = temporalVertex.getPropertyValue("isBlocked").getBoolean();
                    return isBlocked ? new Tuple2<>(0, 1) : new Tuple2<>(1, 0);
                }
            })
            .reduce(
                new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1,
                                                           Tuple2<Integer, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1);
                    }
                }
            );

        Tuple2<Integer, Integer> blockedNonBlockedAccounts = null;

        try {
            List<Tuple2<Integer, Integer>> accountList = accounts.collect();
            if (accountList.isEmpty()) {
                return Collections.singletonList(new SimpleRead3Result(-1.0f));
            }
            blockedNonBlockedAccounts = accountList.get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return Collections.singletonList(
            new SimpleRead3Result(
                roundToDecimalPlaces(
                    (float) blockedNonBlockedAccounts.f0 / (float) blockedNonBlockedAccounts.f1,
                    3
                )
            )
        );
    }
}
