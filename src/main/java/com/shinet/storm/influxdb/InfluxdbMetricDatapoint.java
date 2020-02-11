package com.shinet.storm.influxdb;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class InfluxdbMetricDatapoint implements Serializable {
    private final String metric;
    private final Map<String, String> tags;
    private final Map<String, Object> fields;
    private final long timestamp;
    private final Number value;

    private InfluxdbMetricDatapoint() {
        this((String)null, (Map)null, (Map)null, 0L, (Number)null);
    }
    public InfluxdbMetricDatapoint(String metric, Map<String, String> tags, long timestamp, Number value) {
        this.metric = metric;
        this.tags = Collections.unmodifiableMap(tags);
        this.timestamp = timestamp;
        this.value = value;
        this.fields = Maps.newHashMap();
        if (!(value instanceof Integer) && !(value instanceof Long) && !(value instanceof Float)) {
            throw new RuntimeException("Received tuple contains unsupported value: " + value + " field. It must be Integer/Long/Float.");
        }
    }
    public InfluxdbMetricDatapoint(String metric, Map<String, String> tags, Map<String, Object> fields,long timestamp, Number value) {
        this.metric = metric;
        this.tags = Collections.unmodifiableMap(tags);
        this.fields = Collections.unmodifiableMap(fields);
        this.timestamp = timestamp;
        this.value = value;
        if (!(value instanceof Integer) && !(value instanceof Long) && !(value instanceof Float)) {
            throw new RuntimeException("Received tuple contains unsupported value: " + value + " field. It must be Integer/Long/Float.");
        }
    }

    public String getMetric() {
        return this.metric;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    public Map<String, Object> getFields() {
        return this.fields;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public Object getValue() {
        return this.value;
    }

    public String toString() {
        return "InfluxdbMetricDatapoint{metric='" + this.metric + '\'' + ", tags=" + this.tags + "， fields=" + this.fields + ", timestamp=" + this.timestamp + ", value=" + this.value + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof InfluxdbMetricDatapoint)) {
            return false;
        } else {
            InfluxdbMetricDatapoint that = (InfluxdbMetricDatapoint)o;
            if (this.timestamp != that.timestamp) {
                return false;
            } else if (this.value != that.value) {
                return false;
            } else {
                return !this.metric.equals(that.metric) ? false : this.tags.equals(that.tags);
            }
        }
    }

    public int hashCode() {
        int result = this.metric.hashCode();
        result = 31 * result + this.tags.hashCode();
        result = 31 * result + this.fields.hashCode();
        result = 31 * result + (int)(this.timestamp ^ this.timestamp >>> 32);
        result = 31 * result + this.value.hashCode();
        return result;
    }
}
