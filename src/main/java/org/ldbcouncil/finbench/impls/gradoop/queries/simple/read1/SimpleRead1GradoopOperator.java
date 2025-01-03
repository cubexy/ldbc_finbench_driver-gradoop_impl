package org.ldbcouncil.finbench.impls.gradoop.queries.simple.read1;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

public class SimpleRead1GradoopOperator implements
    UnaryBaseGraphToValueOperator<TemporalGraph, List<SimpleRead1Result>> {
    private final long id;

    public SimpleRead1GradoopOperator(SimpleRead1 simpleRead1) {
        this.id = simpleRead1.getId();
    }

    /**
     * Given an id of an Account, find the properties of the specific Account.
     *
     * @param temporalGraph input graph
     * @return created time, isBlocked status and type of the Account
     */
    @Override
    public List<SimpleRead1Result> execute(TemporalGraph temporalGraph) {
        DataSet<Tuple3<Date, Boolean, String>>
            dataSetResult = temporalGraph.query("MATCH (a:Account) WHERE a.id = " + id + "L")
            .reduce(new ReduceCombination<>())
            .getVertices()
            .map(new MapFunction<TemporalVertex, Tuple3<Date, Boolean, String>>() {
                @Override
                public Tuple3<Date, Boolean, String> map(TemporalVertex temporalVertex) throws Exception {
                    Date createTime =
                        CommonUtils.parseUnixTimeString(temporalVertex.getPropertyValue("createTime").toString());
                    Boolean isBlocked = temporalVertex.getPropertyValue("isBlocked").getBoolean();
                    String type = temporalVertex.getPropertyValue("type").getString();

                    return new Tuple3<>(createTime, isBlocked, type);
                }
            });

        List<SimpleRead1Result> simpleRead1Results = new ArrayList<>();
        try {
            dataSetResult.collect().forEach(
                tuple -> simpleRead1Results.add(new SimpleRead1Result(tuple.f0, tuple.f1, tuple.f2)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return simpleRead1Results;
    }
}
