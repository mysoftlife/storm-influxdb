# Storm connector for InfluxDB

```
InfluxdbClient.Builder influxdbBuilder =  InfluxdbClient.newBuilder("http://172.24.4.138:8086","database").
                measurementPrefix("storm_")
                .enableGzip();
final InfluxdbBolt influxdbBolt = new InfluxdbBolt(influxdbBuilder,Collections.singletonList(TupleInfluxdbDatapointMapper.DEFAULT_MAPPER));
        influxdbBolt
                .withBatchSize(2)
                .withFlushInterval(2000);

```
