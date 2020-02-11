
package com.shinet.storm.influxdb.bolt;

import com.shinet.storm.influxdb.InfluxdbMetricDatapoint;
import com.shinet.storm.influxdb.client.InfluxdbClient;
import com.shinet.storm.influxdb.client.InfluxdbClient.Builder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InfluxdbBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxdbBolt.class);
    private final Builder influxdbClientBuilder;
    private final List<? extends ITupleInfluxdbDataPointMapper> tupleInfluxdbDatapointMappers;
    private int batchSize;
    private int flushIntervalInSeconds;
    private boolean failTupleForFailedMetrics;
    private BatchHelper batchHelper;
    private InfluxdbClient influxdbClient;
    private Map<InfluxdbMetricDatapoint, Tuple> metricPointsWithTuple = new HashMap();
    private OutputCollector collector;

    public InfluxdbBolt(Builder influxdbClientBuilder, ITupleInfluxdbDataPointMapper tupleInfluxdbDatapointMapper) {
        this.influxdbClientBuilder = influxdbClientBuilder;
        this.tupleInfluxdbDatapointMappers = Collections.singletonList(tupleInfluxdbDatapointMapper);
    }

    public InfluxdbBolt(Builder influxdbClientBuilder, List<? extends ITupleInfluxdbDataPointMapper> tupleInfluxdbDatapointMappers) {
        this.influxdbClientBuilder = influxdbClientBuilder;
        this.tupleInfluxdbDatapointMappers = tupleInfluxdbDatapointMappers;
    }

    public InfluxdbBolt withFlushInterval(int flushIntervalInSeconds) {
        this.flushIntervalInSeconds = flushIntervalInSeconds;
        return this;
    }

    public InfluxdbBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public InfluxdbBolt failTupleForFailedMetrics() {
        this.failTupleForFailedMetrics = true;
        return this;
    }
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.batchHelper = new BatchHelper(this.batchSize, collector);
        this.influxdbClient = this.influxdbClientBuilder.build();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (batchHelper.shouldHandle(tuple)) {
                final List<InfluxdbMetricDatapoint> metricDataPoints = getMetricPoints(tuple);
                for (InfluxdbMetricDatapoint metricDataPoint : metricDataPoints) {
                    metricPointsWithTuple.put(metricDataPoint, tuple);
                }
                batchHelper.addBatch(tuple);
            }

            if (batchHelper.shouldFlush()) {
                LOG.debug("Sending metrics of size [{}]", metricPointsWithTuple.size());

                try{
                    this.writeMetricPoints(this.metricPointsWithTuple.keySet());

                    LOG.debug("Acknowledging batched tuples");
                    this.batchHelper.ack();

                }catch (RuntimeException e){

                    for (Tuple batchedTuple : batchHelper.getBatchTuples()) {
                            collector.fail(batchedTuple);
                    }
                    this.failTupleForFailedMetrics();
                    LOG.error("the metric points failed with details: " + e.getMessage());
                }

                metricPointsWithTuple.clear();
            }
        } catch (Exception e) {
            batchHelper.fail(e);
            metricPointsWithTuple.clear();
        }
    }

    private List<InfluxdbMetricDatapoint> getMetricPoints(Tuple tuple) {
        List<InfluxdbMetricDatapoint> metricDataPoints = new ArrayList();
        for (ITupleInfluxdbDataPointMapper tupleInfluxdbDataPointMapper : tupleInfluxdbDatapointMappers) {
            metricDataPoints.add(tupleInfluxdbDataPointMapper.getMetricPoint(tuple));
        }

        return metricDataPoints;
    }

    public void writeMetricPoints(Collection<InfluxdbMetricDatapoint> metricDataPoints){
        for( InfluxdbMetricDatapoint metricDataPoint : metricDataPoints)
        {
            if (metricDataPoint.getValue() != null) {
                this.influxdbClient.prepareDataPoint(metricDataPoint.getMetric(),metricDataPoint.getTags(),metricDataPoint.getFields(),
                        metricDataPoint.getValue(),metricDataPoint.getTimestamp());
            } else {
                LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), metricDataPoint.getMetric());
            }
        }
        this.influxdbClient.sendPoints();
    }


    public void cleanup() {
        this.influxdbClient.closeConnection();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), this.flushIntervalInSeconds);
    }
}
