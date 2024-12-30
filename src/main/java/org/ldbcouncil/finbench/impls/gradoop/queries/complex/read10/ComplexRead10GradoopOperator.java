package org.ldbcouncil.finbench.impls.gradoop.queries.complex.read10;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;
import org.ldbcouncil.finbench.impls.gradoop.CommonUtils;

class ComplexRead10GradoopOperator implements UnaryBaseGraphToValueOperator<TemporalGraph, List<ComplexRead10Result>> {

    private final long id;
    private final long id2;
    private final long startTime;
    private final long endTime;

    public ComplexRead10GradoopOperator(ComplexRead10 cr10) {
        this.id = cr10.getPid1();
        this.id2 = cr10.getPid2();
        this.startTime = cr10.getStartTime().getTime();
        this.endTime = cr10.getEndTime().getTime();
    }

    @Override
    public List<ComplexRead10Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Person", "Company"), new LabelIsIn<>("invest"))
            .fromTo(this.startTime, this.endTime);

        DataSet<Tuple2<Integer, Integer>> jaccard = windowedGraph.query("MATCH (p:Person)-[edge1:invest]->(com:Company)" +
                " WHERE p.id = " + this.id + "L")
            .union(
                windowedGraph.query("MATCH (p:Person)-[edge1:invest]->(com:Company)" +
                    " WHERE p.id = " + this.id2 + "L")
            ).toGraphCollection()
            .getGraphTransactions()
            .map(new MapFunction<GraphTransaction, Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> map(GraphTransaction graphTransaction) throws Exception {
                    Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                    GradoopId pId = m.get("p");
                    GradoopId comId = m.get("com");

                    return new Tuple2<>(graphTransaction.getVertexById(pId).getPropertyValue("id").getLong(), graphTransaction.getVertexById(comId).getPropertyValue("id").getLong());
                }
            })
            .groupBy(1)
            .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> reduce(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2)
                    throws Exception {
                    Long f0 = t1.f0.equals(t2.f0) ? t1.f0 : 0L;
                    return new Tuple2<>(f0, t1.f1);
                }
            })
            .map(new MapFunction<Tuple2<Long, Long>, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> map(Tuple2<Long, Long> t) throws Exception {
                    return new Tuple2<>(1, t.f0.equals(0L) ? 1 : 0);
                }
            })
            .reduce(
                new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1,
                                                           Tuple2<Integer, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0+t2.f0, t1.f1 + t2.f1);
                    }
                }
            );

        Tuple2<Integer, Integer> jaccardCoefficient;

        try {
            jaccardCoefficient = jaccard.collect().get(0); // this throws an error, something is not right --> TODO:
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        float jaccardSimilarity = jaccardCoefficient.f1 == 0 ? 0.0f : (float) jaccardCoefficient.f0 / jaccardCoefficient.f1;


        return Collections.singletonList(new ComplexRead10Result(jaccardSimilarity));
    }
}
