package edu.unl.kafka.connect.swift.wal;

import io.confluent.connect.hdfs.wal.WAL;
import org.apache.kafka.connect.errors.ConnectException;


public class DummyWAL implements WAL {
  @Override
  public void acquireLease() throws ConnectException {

  }

  @Override
  public void append(String tempFile, String committedFile) throws ConnectException {

  }

  @Override
  public void apply() throws ConnectException {

  }

  @Override
  public void truncate() throws ConnectException {

  }

  @Override
  public void close() throws ConnectException {

  }

  @Override
  public String getLogFile() {
    return null;
  }

  public long readOffsetFromWAL() {
    return 0;
  }
}
