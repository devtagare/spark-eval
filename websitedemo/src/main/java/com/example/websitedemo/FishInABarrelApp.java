/**
 * Put your copyright and license info here.
 */
package com.example.websitedemo;

import java.net.URI;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

@ApplicationAnnotation(name = "FishInABarrelApp")
public class FishInABarrelApp implements StreamingApplication
{
  public static final String SNAPSHOT_SCHEMA = "WordDataSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);

    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");

    RandomNumberGenerator randomGenerator = dag.addOperator("TSGenerator", RandomNumberGenerator.class);
    KafkaSinglePortOutputOperator kafkaOutGen = dag.addOperator("kakfaTSOutput", KafkaSinglePortOutputOperator.class);

    KafkaSinglePortInputOperator kafkaInputgen = dag.addOperator("KafkaTSConsumer", KafkaSinglePortInputOperator.class);
    InMemoryCounter inMemorycounter = dag.addOperator("InMemoryCounter", InMemoryCounter.class);
    ApexStoreCounter apexStoreCounter = dag.addOperator("ApexStoreCounter", ApexStoreCounter.class);
    KafkaSinglePortOutputOperator kafkaOut = dag.addOperator("KafkaStoreOutput", KafkaSinglePortOutputOperator.class);
    KafkaSinglePortInputOperator kafkaIn = dag.addOperator("KafkaStoreInput", KafkaSinglePortInputOperator.class);
    RefereeOperator referee = dag.addOperator("referee", RefereeOperator.class);
    kafkaIn.setInitialOffset("LATEST");
    kafkaInputgen.setInitialOffset("LATEST");

    dag.addStream("KafkaTSGenStream", randomGenerator.out, kafkaOutGen.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("InMemoryCounterStream", kafkaInputgen.outputPort, inMemorycounter.input, apexStoreCounter.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("InMemoryRefereeStream", inMemorycounter.output, referee.inMemoryCounterPort);
    dag.addStream("ApexStoreKafkaStream", apexStoreCounter.kafkaOut, kafkaOut.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("KafkaRefereeStream", kafkaIn.outputPort, referee.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

    AppDataSnapshotServerMap snapshotServer = dag.addOperator("SnapshotServer", new AppDataSnapshotServerMap());
    snapshotServer.setSnapshotSchemaJSON(SchemaUtils.jarResourceFileToString(SNAPSHOT_SCHEMA));
    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.setUri(uri);
    wsQuery.enableEmbeddedMode();
    snapshotServer.setEmbeddableQueryInfoProvider(wsQuery);

    PubSubWebSocketAppDataResult wsResult = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsResult.setUri(uri);
    Operator.InputPort<String> queryResultPort = wsResult.input;

    dag.addStream("RefereeSnapshotStream", referee.snapshotOutput, snapshotServer.input);
    dag.addStream("QueryResultStream", snapshotServer.queryResult, queryResultPort);
  }
}


