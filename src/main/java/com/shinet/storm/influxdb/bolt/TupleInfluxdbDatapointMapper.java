

package com.shinet.storm.influxdb.bolt;

import com.shinet.storm.influxdb.InfluxdbMetricDatapoint;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Converts {@link ITuple} to {@link InfluxdbMetricDatapoint}.
 */
public final class TupleInfluxdbDatapointMapper implements ITupleInfluxdbDataPointMapper {
    private static final Logger LOG = LoggerFactory.getLogger(TupleInfluxdbDatapointMapper.class);

    /**
     * Default mapper which can be used when the tuple already contains fields mapping  metric, timestamp, tags and value.
     */
    public static final TupleInfluxdbDatapointMapper DEFAULT_MAPPER =
            new TupleInfluxdbDatapointMapper("metric", "timestamp", "tags", "value");

    private final String metricField;
    private final String timestampField;
    private final String valueField;
    private final String tagsField;

    public TupleInfluxdbDatapointMapper(String metricField, String timestampField, String tagsField, String valueField) {
        this.metricField = metricField;
        this.timestampField = timestampField;
        this.tagsField = tagsField;
        this.valueField = valueField;
    }

    @Override
    public InfluxdbMetricDatapoint getMetricPoint(ITuple tuple) {
        return new InfluxdbMetricDatapoint(
                tuple.getStringByField(metricField),
                (Map<String, String>) tuple.getValueByField(tagsField),
                tuple.getLongByField(timestampField),
                (Number) tuple.getValueByField(valueField));
    }

    /**
     * Retrieve metric field name in the tuple.
     * @return metric field name in the tuple
     */
    public String getMetricField() {
        return metricField;
    }

    /**
     * Retrieve the timestamp field name in the tuple.
     * @return timestamp field name in the tuple
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * Retrieve the value field name in the tuple.
     * @return value field name in the tuple
     */
    public String getValueField() {
        return valueField;
    }

    /**
     * Retrieve the tags field name in the tuple.
     * @return tags field name in the tuple
     */
    public String getTagsField() {
        return tagsField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleInfluxdbDatapointMapper)) {
            return false;
        }

        TupleInfluxdbDatapointMapper that = (TupleInfluxdbDatapointMapper) o;

        if (!metricField.equals(that.metricField)) {
            return false;
        }
        if (!timestampField.equals(that.timestampField)) {
            return false;
        }
        if (!valueField.equals(that.valueField)) {
            return false;
        }
        return tagsField.equals(that.tagsField);
    }

    @Override
    public int hashCode() {
        int result = metricField.hashCode();
        result = 31 * result + timestampField.hashCode();
        result = 31 * result + valueField.hashCode();
        result = 31 * result + tagsField.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TupleInfluxdbDatapointMapper{"
                + "metricField='" + metricField + '\''
                + ", timestampField='" + timestampField + '\''
                + ", valueField='" + valueField + '\''
                + ", tagsField='" + tagsField + '\''
                + '}';
    }
}
