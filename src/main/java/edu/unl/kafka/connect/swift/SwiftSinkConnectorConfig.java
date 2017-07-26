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

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.partitioner.DailyPartitioner;
import io.confluent.connect.hdfs.partitioner.DefaultPartitioner;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;
import io.confluent.connect.hdfs.partitioner.HourlyPartitioner;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SwiftSinkConnectorConfig extends HdfsSinkConnectorConfig {
    public static final String SWIFT_URL_CONFIG = "ocs.url";
    private static final String SWIFT_URL_DOC =
            "Swift destination where data needs to be written. Format swift://container.serviceID/path/to/dir";
    private static final String SWIFT_URL_DISPLAY = "OCS Swift URL";

    public static final String SWIFT_CONTAINER = "ocs.container";
    private static final String SWIFT_CONTAINER_DOC =
            "Container name for Swift Object Store";
    private static final String SWIFT_CONTAINER_DISPLAY = "OCS Swift Container";

    public static final String SWIFT_SERVICE_NAME = "ocs.service.name";
    private static final String SWIFT_SERVICE_NAME_DOC =
            "Swift destination where data needs to be written. Format swift://container.serviceID/path/to/dir";
    private static final String SWIFT_SERVICE_NAME_DISPLAY = "Swift Container";

    public static final String SWIFT_SERVICE_TENANT = "swift.tenant.name";
    private static final String SWIFT_SERVICE_TENANT_DOC =
            "Swift Tenant name";
    private static final String SWIFT_SERVICE_TENANT_DISPLAY = "Swift Tenant Name";

    public static final String SWIFT_SERVICE_PASSWORD = "swift.service.name";
    private static final String SWIFT_SERVICE_PASSWORD_DOC =
            "Password for given service, it could be clear text or java keystore link";
    private static final String SWIFT_SERVICE_PASSWORD_DISPLAY = "Swift Service Password or keystore";


    public static final String WAL_CLASS_CONFIG = "wal.class";
    private static final String WAL_CLASS_DOC =
            "WAL implementation to use. Use RDSWAL if you need exactly once guarantee (applies for swift)";
    public static final String WAL_CLASS_DEFAULT = "edu.unl.kafka.connect.swift.wal.DummyWAL";
    private static final String WAL_CLASS_DISPLAY = "WAL Class";

    public static final String DB_CONNECTION_URL_CONFIG = "db.connection.url";
    private static final String DB_CONNECTION_URL_DOC =
            "JDBC Connection URL. Required when using DBWAL (which is the default WAL implementation for Swift)";
    public static final String DB_CONNECTION_URL_DEFAULT = "";
    private static final String DB_CONNECTION_URL_DISPLAY = "JDBC Connection URL";

    public static final String DB_USER_CONFIG = "db.user";
    private static final String DB_USER_DOC =
            "Name of the User that has access to the database. Required when using DBWAL (which is the default WAL implementation for Swift)";
    public static final String DB_USER_DEFAULT = "";
    private static final String DB_USER_DISPLAY = "DB User";

    public static final String DB_PASSWORD_CONFIG = "db.password";
    private static final String DB_PASSWORD_DOC =
            "Password of the user specified using db.user. Required when using DBWAL (which is the default WAL implementation for Swift)";
    public static final String DB_PASSWORD_DEFAULT = "";
    private static final String DB_PASSWORD_DISPLAY = "DB Password";

    public static final String WAL_GROUP = "DBWAL";
    public static final String SWIFT_GROUP = "swift";

    public static final String NAME_CONFIG = "name";
    private static final String NAME_DOC =
            "Name of the connector";
    public static final String NAME_DEFAULT = "";
    private static final String NAME_DISPLAY = "Connector Name";


    static {
        config.define(SWIFT_URL_CONFIG, Type.STRING, Importance.HIGH, SWIFT_URL_DOC, SWIFT_GROUP, 1, Width.MEDIUM, SWIFT_URL_DISPLAY)
                .define(WAL_CLASS_CONFIG, Type.STRING, WAL_CLASS_DEFAULT, Importance.LOW, WAL_CLASS_DOC, WAL_GROUP, 2, Width.MEDIUM, WAL_CLASS_DISPLAY)
                .define(DB_CONNECTION_URL_CONFIG, Type.STRING, DB_CONNECTION_URL_DEFAULT, Importance.LOW, DB_CONNECTION_URL_DOC, WAL_GROUP, 3, Width.MEDIUM, DB_CONNECTION_URL_DISPLAY)
                .define(DB_USER_CONFIG, Type.STRING, DB_USER_DEFAULT, Importance.LOW, DB_USER_DOC, WAL_GROUP, 4, Width.MEDIUM, DB_USER_DISPLAY)
                .define(DB_PASSWORD_CONFIG, Type.STRING, DB_PASSWORD_DEFAULT, Importance.LOW, DB_PASSWORD_DOC, WAL_GROUP, 5, Width.MEDIUM, DB_PASSWORD_DISPLAY)
                .define(NAME_CONFIG, Type.STRING, NAME_DEFAULT, Importance.HIGH, NAME_DOC, SWIFT_GROUP, 6, Width.MEDIUM, NAME_DISPLAY);
    }


    public SwiftSinkConnectorConfig(Map<String, String> props) {
        super(props);
    }

    public static void main(String[] args) {
    System.out.println(config.toEnrichedRst());
  }
}
