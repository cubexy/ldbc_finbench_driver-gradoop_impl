package org.ldbcouncil.finbench.impls.gradoop;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

public class CommonUtils {

    public static Map<String, GradoopId> getVariableMapping(GraphTransaction gt) {
        Map<String, GradoopId> m = new HashMap<>();

        Map<PropertyValue, PropertyValue> variable_mapping =
            gt.getGraphHead().getPropertyValue("__variable_mapping").getMap();

        variable_mapping.forEach((k, v) -> m.put(k.getString(), v.getGradoopId()));
        return m;
    }

    public static Date parseUnixTimeString(String time) throws NumberFormatException, ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.GERMAN);
        df.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
        return df.parse(time);
    }

    public static Float getDataSetSum(DataSet<Tuple1<Double>> dataSet, int decimalPlaces) {
        try {
            double sum = dataSet.sum(0).collect().iterator().next().f0;
            return BigDecimal.valueOf(sum).setScale(decimalPlaces, RoundingMode.HALF_UP).floatValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Float getDataSetMax(DataSet<Tuple1<Double>> dataSet, int decimalPlaces) {
        try {
            double max = dataSet.max(0).collect().iterator().next().f0;
            return BigDecimal.valueOf(max).setScale(decimalPlaces, RoundingMode.HALF_UP).floatValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
