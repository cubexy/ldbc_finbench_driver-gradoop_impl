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

    /**
     * Given two Persons and a specified time window between startTime and endTime, find all the Com-
     * panies the two Persons invest in. Return the Jaccard similarity between the two companies set.
     * Return 0 if there is no edges found connecting to any of these two persons.
     *
     * @param temporalGraph input graph
     * @return Jaccard similarity between the two companies set
     */
    @Override
    public List<ComplexRead10Result> execute(TemporalGraph temporalGraph) {
        TemporalGraph windowedGraph = temporalGraph
            .subgraph(new LabelIsIn<>("Person", "Company"), new LabelIsIn<>("invest"))
            .fromTo(this.startTime, this.endTime);

        DataSet<Tuple2<Integer, Integer>> jaccard =
            windowedGraph.query("MATCH (p:Person)-[edge1:invest]->(com:Company)" +
                    " WHERE p.id = " + this.id + "L OR p.id = " + this.id2 + "L")
                .toGraphCollection()
                .getGraphTransactions()
                .map(new MapFunction<GraphTransaction, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(GraphTransaction graphTransaction) {
                        Map<String, GradoopId> m = CommonUtils.getVariableMapping(graphTransaction);

                        long pId = graphTransaction.getVertexById(m.get("p")).getPropertyValue("id").getLong();
                        long comId = graphTransaction.getVertexById(m.get("com")).getPropertyValue("id").getLong();

                        return new Tuple2<>(pId, comId); // Get each pair of (pId, comId)
                    }
                })
                .distinct(0, 1) // Only keep distinct pairs
                .groupBy(1) // Group by company ID
                .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> t1, Tuple2<Long, Long> t2) {
                        return new Tuple2<>(0L, t1.f1); // If reduce is called, the company has to be linked to both persons -> replace person ID with 0
                    }
                })
                .map(new MapFunction<Tuple2<Long, Long>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Long, Long> t) {
                        return new Tuple2<>(1, t.f0.equals(0L) ? 1 : 0); // Tuple is (total number of companies, number of companies that both persons invested in)
                    }
                })
                .reduce(
                    new ReduceFunction<Tuple2<Integer, Integer>>() {
                        @Override
                        public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1,
                                                               Tuple2<Integer, Integer> t2) {
                            return new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1); // aggregate the results
                        }
                    }
                );

        Tuple2<Integer, Integer> jaccardCoefficient;

        try {
            final List<Tuple2<Integer, Integer>> jaccardResult = jaccard.collect();
            jaccardCoefficient = !jaccardResult.isEmpty() ? jaccardResult.get(0) : new Tuple2<>(0, 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        float jaccardSimilarity = CommonUtils.roundToDecimalPlaces(
            jaccardCoefficient.f1 == 0 ? 0.0f : (float) jaccardCoefficient.f0 / jaccardCoefficient.f1, 3);


        return Collections.singletonList(new ComplexRead10Result(jaccardSimilarity));
    }
}
