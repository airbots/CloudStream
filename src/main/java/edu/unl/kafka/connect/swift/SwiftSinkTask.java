/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * Chen He airbots@gmail.com
 **/

package edu.unl.kafka.connect.swift;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.HdfsSinkTask;
import io.confluent.connect.hdfs.Version;
import io.confluent.connect.hdfs.schema.Compatibility;
import io.confluent.connect.hdfs.schema.SchemaUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SwiftSinkTask extends HdfsSinkTask {

  private static final Logger log = LoggerFactory.getLogger(SwiftSinkTask.class);
  private DataWriter swiftWriter;
  private AvroData avroData;

  public SwiftSinkTask() {

  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    Set<TopicPartition> assignment = context.assignment();
    try {
      SwiftSinkConnectorConfig connectorConfig = new SwiftSinkConnectorConfig(props);
      boolean hiveIntegration = connectorConfig.getBoolean(SwiftSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
      if (hiveIntegration) {
        Compatibility compatibility = SchemaUtils.getCompatibility(
            connectorConfig.getString(SwiftSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));
        if (compatibility == Compatibility.NONE) {
          throw new ConfigException("Hive Integration requires schema compatibility to be BACKWARD, FORWARD or FULL");
        }
      }

      //check that timezone it setup correctly in case of scheduled rotation
      if(connectorConfig.getLong(SwiftSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG) > 0) {
        String timeZoneString = connectorConfig.getString(SwiftSinkConnectorConfig.TIMEZONE_CONFIG);
        if (timeZoneString.equals("")) {
          throw new ConfigException(SwiftSinkConnectorConfig.TIMEZONE_CONFIG,
                  timeZoneString, "Timezone cannot be empty when using scheduled file rotation.");
        }
        DateTimeZone.forID(timeZoneString);
      }

      int schemaCacheSize = connectorConfig.getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
      avroData = new AvroData(schemaCacheSize);
      swiftWriter = new DataWriter(connectorConfig, context, avroData);
      recover(assignment);
      if (hiveIntegration) {
        syncWithHive();
      }
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start SwiftSinkConnector due to configuration error.", e);
    } catch (ConnectException e) {
      log.info("Couldn't start SwiftSinkConnector:", e);
      log.info("Shutting down SwiftSinkConnector.");
      if (swiftWriter != null) {
        swiftWriter.close(assignment);
        swiftWriter.stop();
      }
    }
  }

  @Override
  public void stop() throws ConnectException {
    if (swiftWriter != null) {
      swiftWriter.stop();
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    try {
      swiftWriter.write(records);
    } catch (ConnectException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // Do nothing as the connector manages the offset
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    swiftWriter.open(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
      log.debug("TopicPartition size is:"+partitions.size());
      log.debug("SwiftWriter is: "+ swiftWriter.toString());
    swiftWriter.close(partitions);
  }

  private void recover(Set<TopicPartition> assignment) {
    for (TopicPartition tp: assignment) {
      swiftWriter.recover(tp);
    }
  }

  private void syncWithHive() throws ConnectException {
    swiftWriter.syncWithHive();
  }

  public AvroData getAvroData() {
    return avroData;
  }
}
