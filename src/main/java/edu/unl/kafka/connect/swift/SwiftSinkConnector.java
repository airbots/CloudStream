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
 *  Chen He airbots@gmail.com
 */

package edu.unl.kafka.connect.swift;


import io.confluent.connect.hdfs.HdfsSinkConnector;
import io.confluent.connect.hdfs.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SwiftSinkConnector is a Kafka Connect Connector implementation that ingest data from Kafka to HDFS.
 */
public class SwiftSinkConnector extends HdfsSinkConnector {

  private static final Logger log = LoggerFactory.getLogger(SwiftSinkConnector.class);
  private Map<String, String> configProperties;
  private SwiftSinkConnectorConfig config;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    try {
      configProperties = props;
      config = new SwiftSinkConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start SwiftSinkConnector due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SwiftSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {

  }

  @Override
  public ConfigDef config() {
    return SwiftSinkConnectorConfig.getConfig();
  }
}
