
package com.shinet.storm.influxdb.client;

import com.google.common.collect.Maps;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxdbClient {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxdbClient.class);
    public static final String KEY_INFLUXDB_URL = "metrics.influxdb.url";
    public static final String KEY_INFLUXDB_USERNAME = "metrics.influxdb.username";
    public static final String KEY_INFLUXDB_PASSWORD = "metrics.influxdb.password";
    public static final String KEY_INFLUXDB_DATABASE = "metrics.influxdb.database";
    public static final String KEY_INFLUXDB_MEASUREMENT_PREFIX = "metrics.influxdb.measurement.prefix";
    public static final String KEY_INFLUXDB_ENABLE_GZIP = "metrics.influxdb.enable.gzip";
    public static final String DEFAULT_INFLUXDB_URL = "http://localhost:8086";
    public static final String DEFAULT_INFLUXDB_USERNAME = "";
    public static final String DEFAULT_INFLUXDB_PASSWORD = "";
    public static final String DEFAULT_INFLUXDB_DATABASE = "apache-storm-metrics";
    public static final String DEFAULT_INFLUXDB_MEASUREMENT_PREFIX = "storm-";
    public static final Boolean DEFAULT_INFLUXDB_ENABLE_GZIP = true;
    private InfluxDB influxDB;
    private BatchPoints batchPoints;
    private String influxdbUrl;
    private String influxdbUsername;
    private String influxdbPassword;
    private String influxdbDatabase;
    private String influxdbMeasurementPrefix;
    private Boolean influxdbEnableGzip;
    private boolean databaseWasCreated = false;

    public InfluxdbClient(Map<Object, Object> config) {
        LOG.debug("{}: config = {}", this.getClass().getSimpleName(), config.toString());
        this.influxdbUrl = (String)this.getKeyValueOrDefaultValue(config, KEY_INFLUXDB_URL, DEFAULT_INFLUXDB_URL);
        this.influxdbUsername = (String)this.getKeyValueOrDefaultValue(config, KEY_INFLUXDB_USERNAME, DEFAULT_INFLUXDB_USERNAME);
        this.influxdbPassword = (String)this.getKeyValueOrDefaultValue(config, KEY_INFLUXDB_PASSWORD, DEFAULT_INFLUXDB_PASSWORD);
        this.influxdbDatabase = (String)this.getKeyValueOrDefaultValue(config, KEY_INFLUXDB_DATABASE, DEFAULT_INFLUXDB_DATABASE);
        this.influxdbMeasurementPrefix = (String)this.getKeyValueOrDefaultValue(config, KEY_INFLUXDB_MEASUREMENT_PREFIX, DEFAULT_INFLUXDB_MEASUREMENT_PREFIX);
        this.influxdbEnableGzip = (Boolean)this.getKeyValueOrDefaultValue(config, KEY_INFLUXDB_ENABLE_GZIP, DEFAULT_INFLUXDB_ENABLE_GZIP);
        this.prepareConnection();
    }

    private Object getKeyValueOrDefaultValue(Map<Object, Object> objects, String key, Object defaultValue) {
        if (objects.containsKey(key)) {
            return objects.get(key);
        } else {
            LOG.warn("{}: Using default parameter for {}", this.getClass().getSimpleName(), key);
            return defaultValue;
        }
    }

    public void prepareConnection() {
        if (influxDB == null) {
            LOG.debug("{}: Preparing connection to InfluxDB: [ url='{}', username='{}', password='{}' ]",
                    this.getClass().getSimpleName(),
                    this.influxdbUrl,
                    this.influxdbUsername,
                    this.influxdbPassword
            );

            if (this.influxdbUsername.isEmpty() && this.influxdbPassword.isEmpty()) {
                this.influxDB = InfluxDBFactory.connect(this.influxdbUrl);
            } else {
                this.influxDB = InfluxDBFactory.connect(this.influxdbUrl, this.influxdbUsername, this.influxdbPassword);
            }

            // additional connections options
            if (this.influxdbEnableGzip) {
                this.influxDB.enableGzip();
            }
        } else {
            LOG.debug("{}: InfluxDB connection was available: [ url='{}', username='{}', password='{}' ]",
                    this.getClass().getSimpleName(),
                    this.influxdbUrl,
                    this.influxdbUsername,
                    this.influxdbPassword
            );
        }
    }

    /**
     * Create the database if not exist
     */
    void createDatabaseIfNotExists() {
        if (!this.databaseWasCreated) {

            LOG.debug("{}: Creating database with name = {}", this.getClass().getSimpleName(), this.influxdbDatabase);

            this.influxDB.createDatabase(this.influxdbDatabase);
            this.databaseWasCreated = true;
        }
    }

    /**
     * Create a BatchPoints
     */
    void prepareBatchPoints() {
        this.batchPoints = BatchPoints
                .database(this.influxdbDatabase)
                .retentionPolicy("autogen")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
    }

    public void prepareDataPoint(String name,Map<String, String> tags,Map<String, Object> fields, Object value,long time) {
        if (this.batchPoints == null) {
            this.prepareBatchPoints();
        }

        String measurement = this.influxdbMeasurementPrefix + name;
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}: DataPoint name={} has value type={}", new Object[]{this.getClass().getSimpleName(), name, value.getClass().getName()});
        }

        Point point;
        if (value instanceof String) {
            point = Point.measurement(measurement).addField("value", (String)value).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
            this.batchPoints.point(point);
        }else if (value instanceof Number) {
            if (!Float.valueOf(((Number)value).floatValue()).isNaN() && !Float.valueOf(((Number)value).floatValue()).isInfinite()) {
                point = Point.measurement(measurement).addField("value", (double)((Number)value).floatValue()).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
                this.batchPoints.point(point);
            } else {
                LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), measurement);
            }
        } else if (value instanceof Float) {
            if (!((Float)value).isNaN() && !((Float)value).isInfinite()) {
                point = Point.measurement(measurement).addField("value", (Float)value).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
                this.batchPoints.point(point);
            } else {
                LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), measurement);
            }
        } else if (value instanceof Integer) {
            point = Point.measurement(measurement).addField("value", (Integer)value).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
            this.batchPoints.point(point);
        } else if (value instanceof Boolean) {
            point = Point.measurement(measurement).addField("value", (Boolean)value).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
            this.batchPoints.point(point);
        } else if (value instanceof Long) {
            if (!Float.valueOf(((Long)value).floatValue()).isNaN() && !Float.valueOf(((Long)value).floatValue()).isInfinite()) {
                point = Point.measurement(measurement).addField("value", (double)((Long)value).floatValue()).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
                this.batchPoints.point(point);
            } else {
                LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), measurement);
            }
        } else if (value instanceof Double) {
            if (!Float.valueOf(((Double)value).floatValue()).isNaN() && !Float.valueOf(((Double)value).floatValue()).isInfinite()) {
                point = Point.measurement(measurement).addField("value", (double)((Double)value).floatValue()).fields(fields).tag(tags).time(time, TimeUnit.NANOSECONDS).build();
                this.batchPoints.point(point);
            } else {
                LOG.warn("{}: Discarding dataPoint: {}, value is null", this.getClass().getSimpleName(), measurement);
            }
        }  else {
            LOG.warn("{}: Unable to parse the Java type of 'value' : [type:'{}' value:'{}' time:'{}']", new Object[]{this.getClass().getSimpleName(), name, value.getClass().getSimpleName(),time});
        }

    }
    /**
     * Send Points to InfluxDB server
     */

    public void sendPoints() {

        this.createDatabaseIfNotExists();

        if (this.batchPoints != null) {

            LOG.debug("{}: Sending points to database = {}", this.getClass().getSimpleName(), this.influxdbDatabase);

            this.influxDB.write(this.batchPoints);
            this.batchPoints = null;
        } else {
            LOG.warn("No points values to send");
        }
    }

    /**
     * Close connection to InfluxDB server
     */
    public void closeConnection() {

        LOG.debug("{}: Closing connection to database = {}", this.getClass().getSimpleName(), this.influxdbDatabase);

        this.influxDB.close();
    }
    public static InfluxdbClient.Builder newBuilder(String influxdbUrl,String influxdbDatabase) {

        return new InfluxdbClient.Builder(influxdbUrl,influxdbDatabase,"","");
    }
    public static InfluxdbClient.Builder newBuilder(String influxdbUrl,String influxdbDatabase,  String influxdbUsername, String influxdbPassword) {
        return new InfluxdbClient.Builder(influxdbUrl,influxdbDatabase,influxdbUsername,influxdbPassword);
    }

    public static class Builder implements Serializable {

        private String influxdbUrl;
        private String influxdbUsername;
        private String influxdbPassword;
        private String influxdbDatabase;
        private String influxdbMeasurementPrefix;
        private Boolean influxdbEnableGzip  = DEFAULT_INFLUXDB_ENABLE_GZIP;

        public  Builder(String influxdbUrl, String influxdbDatabase, String influxdbUsername, String influxdbPassword) {
            this.influxdbUrl =influxdbUrl;
            this.influxdbDatabase = influxdbDatabase;
            this.influxdbUsername  = influxdbUsername;
            this.influxdbPassword = influxdbPassword;
        }
        public InfluxdbClient.Builder measurementPrefix(String influxdbMeasurementPrefix) {
            this.influxdbMeasurementPrefix = influxdbMeasurementPrefix;
            return this;
        }
        public InfluxdbClient.Builder enableGzip() {
            this.influxdbEnableGzip = true;
            return this;
        }

        public InfluxdbClient build() {
            Map<Object, Object> config = Maps.newHashMap();

            config.put(KEY_INFLUXDB_URL,this.influxdbUrl);
            config.put(KEY_INFLUXDB_USERNAME,this.influxdbUsername);
            config.put(KEY_INFLUXDB_PASSWORD,this.influxdbPassword);
            config.put(KEY_INFLUXDB_DATABASE,this.influxdbDatabase);
            config.put(KEY_INFLUXDB_MEASUREMENT_PREFIX,this.influxdbMeasurementPrefix);
            config.put(KEY_INFLUXDB_ENABLE_GZIP,this.influxdbEnableGzip);

            return new InfluxdbClient(config);
        }


    }
}
