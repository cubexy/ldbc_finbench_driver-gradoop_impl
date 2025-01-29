package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read3;

import static org.ldbcouncil.finbench.impls.gradoop.CommonUtils.roundToDecimalPlaces;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

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

    /**
     * Given an Account, find the ratio of transfer-ins from blocked Accounts in all its transfer-ins in a specific
     * time range between startTime and endTime. Return the ratio. Return -1 if there is no transfer-ins to
     * the given account.
     *
     * @param temporalGraph input graph
     * @return ratio of transfer-ins from blocked Accounts in all its transfer-ins in a specific time range
     */
    @Override
    public List<SimpleRead3Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Account"), new LabelIsIn<>("transfer"))
            .fromTo(this.startTime.getTime(), this.endTime.getTime());

        final long id_serializable =
            this.id; // this is necessary because this.id is not serializable, which is needed for the transformVertices function

        DataSet<GraphTransaction> accountBaseGraph = windowedGraph.query(
                "MATCH (src:Account)-[edge:transfer]->(dst:Account) WHERE dst.id =" +
                    id_serializable +
                    "L AND edge.amount > " + this.threshold)
            .toGraphCollection()
            .getGraphTransactions();


        DataSet<Tuple2<Integer, Integer>> accounts = accountBaseGraph
            .map(new MapFunction<GraphTransaction, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> map(GraphTransaction graphTransaction) {
                    // each transaction looks like this:
                    // srcAccount - transfer -> dstAccount
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);
                    EPGMVertex src = graphTransaction.getVertexById(m.get("src"));
                    final boolean isBlocked = src.getPropertyValue("isBlocked").getBoolean();
                    return isBlocked ? new Tuple2<>(1, 1) : new Tuple2<>(0, 1);
                }
            })
            .reduce(
                new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1,
                                                           Tuple2<Integer, Integer> t2) {
                        return new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1);
                    }
                }
            );

        Tuple2<Integer, Integer> blockedNonBlockedAccounts;

        try {
            List<Tuple2<Integer, Integer>> accountList = accounts.collect();
            if (accountList.isEmpty() || accountList.get(0).f0 == 0 && accountList.get(0).f1 == 0) {
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
