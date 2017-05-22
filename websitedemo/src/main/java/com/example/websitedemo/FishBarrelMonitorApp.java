package com.example.websitedemo;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;

public class FishBarrelMonitorApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    SparkMonitorOperator sparkMonitor = dag.addOperator("sparkMonitorOperator",SparkMonitorOperator.class);

    dag.addStream("sparkMonitorStream",kafkaInput.outputPort,sparkMonitor.input);
  }
}
