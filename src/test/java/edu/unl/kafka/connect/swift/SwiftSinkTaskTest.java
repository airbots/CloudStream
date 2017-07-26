package edu.unl.kafka.connect.swift;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.avro.AvroFileReader;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.hdfs.storage.StorageFactory;
import io.confluent.connect.hdfs.wal.WAL;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by chehe on 2017/4/25.
 */
public class SwiftSinkTaskTest extends SwiftSinkConnectorTestBase {
  private static final String DIRECTORY1 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
  private static final String DIRECTORY2 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION2);
  private static final String extension = ".avro";
  private static final String ZERO_PAD_FMT = "%010d";
  private final SchemaFileReader schemaFileReader = new AvroFileReader(avroData);

  protected FileSystem fs;

  @Before
  public void setUp() throws Exception {
    url = "swift://oehcs.chehe/";
    super.setUp();
    conf.set("fs.defaultFS", url);
    fs = FileSystem.get(conf);
    Map<String, String> props = createProps();
    connectorConfig = new SwiftSinkConnectorConfig(props);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    super.tearDown();
  }


  @Test
  public void testSinkTaskStart() throws Exception {
    createCommittedFiles();

    Map<String, String> props = createProps();
    SwiftSinkTask task = new SwiftSinkTask();

    task.initialize(context);
    System.out.println("CHEN: SwiftSinkTask connectorConfig: " + props.get("fs.swift.service.default.auth.url"));
    task.start(props);

    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(offsets.size(), 2);
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    //assertEquals(21, (long) offsets.get(TOPIC_PARTITION));
    assertTrue(offsets.containsKey(TOPIC_PARTITION2));
    assertEquals(46, (long) offsets.get(TOPIC_PARTITION2));

    task.stop();
  }

  private void createCommittedFiles() throws IOException {
    String file1 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 0,
        10, extension, ZERO_PAD_FMT);
    String file2 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 11,
        20, extension, ZERO_PAD_FMT);
    String file3 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION2, 21,
        40, extension, ZERO_PAD_FMT);
    String file4 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION2, 41,
        45, extension, ZERO_PAD_FMT);
    fs.createNewFile(new Path(file1));
    fs.createNewFile(new Path(file2));
    fs.createNewFile(new Path(file3));
    fs.createNewFile(new Path(file4));
  }

  private void createWALs(Map<TopicPartition, List<String>> tempfiles,
                          Map<TopicPartition, List<String>> committedFiles) throws Exception {
    @SuppressWarnings("unchecked")
    Class<? extends Storage> storageClass = (Class<? extends Storage>)
        Class.forName(connectorConfig.getString(SwiftSinkConnectorConfig.STORAGE_CLASS_CONFIG));
    Storage storage = StorageFactory.createStorage(storageClass, conf, connectorConfig, url);

    for (TopicPartition tp: tempfiles.keySet()) {
      WAL wal = storage.wal(logsDir, tp);
      List<String> tempList = tempfiles.get(tp);
      List<String> committedList = committedFiles.get(tp);
      wal.append(WAL.beginMarker, "");
      for (int i = 0; i < tempList.size(); ++i) {
        wal.append(tempList.get(i), committedList.get(i));
      }
      wal.append(WAL.endMarker, "");
      wal.close();
    }
  }

  @Override
  protected Map<String, String> createProps() {
    return super.createProps();
  }

  //private void set(){
  //  ByteBuffer
  //}
}
