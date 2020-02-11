package com.shinet.storm.influxdb.bolt;

import com.shinet.storm.influxdb.InfluxdbMetricDatapoint;
import org.apache.storm.tuple.ITuple;

import java.io.Serializable;

public interface ITupleInfluxdbDataPointMapper extends Serializable {
    InfluxdbMetricDatapoint getMetricPoint(ITuple tuple);
}
