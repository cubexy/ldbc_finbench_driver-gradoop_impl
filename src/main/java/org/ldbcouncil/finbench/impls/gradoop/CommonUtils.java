package org.ldbcouncil.finbench.impls.gradoop;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.ldbcouncil.finbench.driver.result.Path;

/**
 * Common utility functions for the Gradoop implementation.
 */
public class CommonUtils {

    /**
     * Returns the variable mapping (variable names for vertices) from the given GraphTransaction.
     *
     * @param gt GraphTransaction
     * @return variable mapping
     */
    public static Map<String, GradoopId> getVariableMapping(GraphTransaction gt) {
        Map<String, GradoopId> m = new HashMap<>();

        Map<PropertyValue, PropertyValue> variable_mapping =
            gt.getGraphHead().getPropertyValue("__variable_mapping").getMap();

        variable_mapping.forEach((k, v) -> m.put(k.getString(), v.getGradoopId()));
        return m;
    }

    /**
     * Parses a Unix timestamp string to a Date object.
     *
     * @param time Unix timestamp string
     * @return Date object
     * @throws NumberFormatException error while parsing the timestamp
     * @throws ParseException        error while parsing the timestamp
     */
    public static Date parseUnixTimeString(String time) throws NumberFormatException, ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.GERMAN);
        df.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
        return df.parse(time);
    }

    /**
     * Parses a path from a list of IDs to a Path object. If an ID is -1L, it is skipped.
     *
     * @param path list of IDs (e.g. account IDs)
     * @return path for the given list of IDs
     */
    public static Path parsePath(List<Long> path) {
        Path p = new Path();
        for (Long l : path) {
            if (l == -1L) {
                continue;
            }
            p.addId(l);
        }
        return p;
    }

    /**
     * Sets the initial parallelism for the given connection state. This is needed to reset env parallelism after a query sets it to a different value.
     *
     * @param connectionState connection state
     */
    public static void setInitialParallelism(GradoopFinbenchBaseGraphState connectionState) {
        connectionState.getGraph().getConfig().getExecutionEnvironment()
            .setParallelism(connectionState.getParallelism());
    }

    /**
     * Rounds a double to the specified number of decimal places.
     *
     * @param num           number to round
     * @param decimalPlaces number of decimal places
     * @return rounded number
     */
    public static Double roundToDecimalPlaces(Double num, int decimalPlaces) {
        try {
            return new BigDecimal(num).setScale(decimalPlaces, RoundingMode.HALF_UP).doubleValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Rounds a float to the specified number of decimal places.
     *
     * @param num           number to round
     * @param decimalPlaces number of decimal places
     * @return rounded number
     */
    public static Float roundToDecimalPlaces(Float num, int decimalPlaces) {
        try {
            return new BigDecimal(num).setScale(decimalPlaces, RoundingMode.HALF_UP).floatValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
