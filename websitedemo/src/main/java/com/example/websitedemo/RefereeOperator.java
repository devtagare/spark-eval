package com.example.websitedemo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class RefereeOperator extends BaseOperator
{
  @AutoMetric
  private float apex_speed_pct = 0;
  @AutoMetric
  private float apex_store_speed_pct = 0;
  @AutoMetric
  private float spark_streaming_speed_pct = 0;
  @AutoMetric
  private long apexWins = 0;
  @AutoMetric
  private long apexStorageWins = 0;
  @AutoMetric
  private long sparkStorageWins = 0;
  @AutoMetric
  private float apexAverageLatency = 0;
  @AutoMetric
  private long apexCumulativeLag = 0;
  @AutoMetric
  private float apexStorageAverageLatency = 0;
  @AutoMetric
  private long apexStorageCumulativeLag = 0;
  @AutoMetric
  private float sparkAverageLatency = 0;
  @AutoMetric
  private long sparkCumulativeLag = 0;
  @AutoMetric
  private long appStartTimeStamp = 0;

  public final transient DefaultOutputPort<List<Map<String, Object>>> snapshotOutput = new DefaultOutputPort<>();

  private static final Logger LOG = LoggerFactory.getLogger(RefereeOperator.class);

  private long totalEventsProcessed = 0;

  public List<Map<String, Object>> snapshotList = new ArrayList<Map<String, Object>>(8);

  //Map<String, Object> resultMap = new HashMap<String, Object>(1000, 2.0f);

  private transient TLongObjectHashMap processingMap = new TLongObjectHashMap(1000, 1.0f);

  public RefereeOperator()
  {
  }

  public long getApexWins()
  {
    return apexWins;
  }

  public void setApexWins(long apexWins)
  {
    this.apexWins = apexWins;
  }

  public long getApexStorageWins()
  {
    return apexStorageWins;
  }

  public void setApexStorageWins(long apexStorageWins)
  {
    this.apexStorageWins = apexStorageWins;
  }

  public long getSparkStorageWins()
  {
    return sparkStorageWins;
  }

  public void setSparkStorageWins(long sparkStorageWins)
  {
    this.sparkStorageWins = sparkStorageWins;
  }

  public float getApexAverageLatency()
  {
    return apexAverageLatency;
  }

  public void setApexAverageLatency(float apexAverageLatency)
  {
    this.apexAverageLatency = apexAverageLatency;
  }

  public long getApexCumulativeLag()
  {
    return apexCumulativeLag;
  }

  public void setApexCumulativeLag(long apexCumulativeLag)
  {
    this.apexCumulativeLag = apexCumulativeLag;
  }

  public float getSparkAverageLatency()
  {
    return sparkAverageLatency;
  }

  public void setSparkAverageLatency(float sparkAverageLatency)
  {
    this.sparkAverageLatency = sparkAverageLatency;
  }

  public long getSparkCumulativeLag()
  {
    return sparkCumulativeLag;
  }

  public void setSparkCumulativeLag(long sparkCumulativeLag)
  {
    this.sparkCumulativeLag = sparkCumulativeLag;
  }

  public TLongObjectHashMap getProcessingMap()
  {
    return processingMap;
  }

  public void setProcessingMap(TLongObjectHashMap processingMap)
  {
    this.processingMap = processingMap;
  }

  public DefaultInputPort<byte[]> getInput()
  {
    return input;
  }

  public void setInput(DefaultInputPort<byte[]> input)
  {
    this.input = input;
  }

  public DefaultInputPort<Object> getInMemoryCounterPort()
  {
    return inMemoryCounterPort;
  }

  public void setInMemoryCounterPort(DefaultInputPort<Object> inMemoryCounterPort)
  {
    this.inMemoryCounterPort = inMemoryCounterPort;
  }

  public float getApexStorageAverageLatency()
  {
    return apexStorageAverageLatency;
  }

  public void setApexStorageAverageLatency(float apexStorageAverageLatency)
  {
    this.apexStorageAverageLatency = apexStorageAverageLatency;
  }

  public long getApexStorageCumulativeLag()
  {
    return apexStorageCumulativeLag;
  }

  public void setApexStorageCumulativeLag(long apexStorageCumulativeLag)
  {
    this.apexStorageCumulativeLag = apexStorageCumulativeLag;
  }

  public long getTotalEventsProcessed()
  {
    return totalEventsProcessed;
  }

  public void setTotalEventsProcessed(long totalEventsProcessed)
  {
    this.totalEventsProcessed = totalEventsProcessed;
  }

  public long getAppStartTimeStamp()
  {
    return appStartTimeStamp;
  }

  public void setAppStartTimeStamp(long appStartTimeStamp)
  {
    this.appStartTimeStamp = appStartTimeStamp;
  }

  public float getApex_speed_pct()
  {
    return apex_speed_pct;
  }

  public void setApex_speed_pct(float apex_speed_pct)
  {
    this.apex_speed_pct = apex_speed_pct;
  }

  public float getApex_store_speed_pct()
  {
    return apex_store_speed_pct;
  }

  public void setApex_store_speed_pct(float apex_store_speed_pct)
  {
    this.apex_store_speed_pct = apex_store_speed_pct;
  }

  public float getSpark_streaming_speed_pct()
  {
    return spark_streaming_speed_pct;
  }

  public void setSpark_streaming_speed_pct(float spark_streaming_speed_pct)
  {
    this.spark_streaming_speed_pct = spark_streaming_speed_pct;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    appStartTimeStamp = System.currentTimeMillis();
    super.setup(context);
  }

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

  public transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      String[] ts = new String(tuple).split(",");
      Event event = new Event();
      event.setInitialTimeStamp(Long.parseLong(ts[1]));
      event.setFinalTimeStamp(System.currentTimeMillis()+Long.parseLong(ts[3]));
      event.setBatchCount(Long.parseLong(ts[2]));
      event.setTypeOfApp(Integer.parseInt(ts[0]));
      //if (LOG.isDebugEnabled()) {
        LOG.info("Timestamp of the event with  type {} timestamp {} and # count {} arrived at Referee is {}", event.getTypeOfApp(), event.getInitialTimeStamp(), event.getBatchCount(), System.currentTimeMillis());
      //}
      processTuple(event);
    }
  };

  public transient DefaultInputPort<Object> inMemoryCounterPort = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      processTuple((Event)tuple);
    }
  };

  public void processTuple(Event tuple)
  {
    long initialTsKey = tuple.getInitialTimeStamp();
    TIntLongHashMap initialTsValue = (TIntLongHashMap)processingMap.get(tuple.getBatchCount());
    if (initialTsValue == null) {
      initialTsValue = new TIntLongHashMap();
      initialTsValue.put(tuple.getTypeOfApp(), tuple.getFinalTimeStamp() - initialTsKey);
      processingMap.put(tuple.getBatchCount(), initialTsValue);
    } else {
      initialTsValue.put(tuple.getTypeOfApp(), tuple.getFinalTimeStamp() - initialTsKey);
      if (initialTsValue.size() == 3) {
        Map<Long, Integer> sortedMap = new TreeMap<Long, Integer>();
        totalEventsProcessed++;
        for (TIntLongIterator it = initialTsValue.iterator(); it.hasNext(); ) {
          it.advance();
          if (it.key() == 0) {
            apexCumulativeLag = apexCumulativeLag + it.value();
            apexAverageLatency = (float)apexCumulativeLag / (totalEventsProcessed);
            sortedMap.put(it.value(), 0);
          } else if (it.key() == 1) {
            apexStorageCumulativeLag = apexStorageCumulativeLag + it.value();
            apexStorageAverageLatency = (float)apexStorageCumulativeLag / (totalEventsProcessed);
            sortedMap.put(it.value(), 1);
          } else {
            sparkCumulativeLag = sparkCumulativeLag + it.value();
            sparkAverageLatency = (float)sparkCumulativeLag / (totalEventsProcessed);
            sortedMap.put(it.value(), 2);
          }
        }
        int points = 1;
        Iterator it = sortedMap.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          if (pair.getValue() == 0) {
            apexWins = apexWins + points;
            if (points != 0) {
              points = points - 1;
            }
          }
          if (pair.getValue() == 1) {
            apexStorageWins = apexStorageWins + points;
            if (points != 0) {
              points = points - 1;
            }
          }
          if (pair.getValue() == 2) {
            sparkStorageWins = sparkStorageWins + points;
            if (points != 0) {
              points = points - 1;
            }
          }
        }
        snapshotList.clear();
        Map<String, Object> resultMap = new HashMap<String, Object>(13, 1.0f);

        apex_speed_pct = (float)sparkAverageLatency / apexAverageLatency;
        apex_store_speed_pct = (float)sparkAverageLatency / apexStorageAverageLatency;
        spark_streaming_speed_pct = (float)sparkAverageLatency / sparkAverageLatency;

        resultMap.put("app_start_time", Long.toString(appStartTimeStamp));
        resultMap.put("apex_wins", Long.toString(apexWins));
        resultMap.put("apex_lag_avg", String.format("%2.02f", apexAverageLatency));
        resultMap.put("apex_speed_pct", String.format("%2.02f", apex_speed_pct));
        resultMap.put("apex_lag_sum", Long.toString(apexCumulativeLag));
        resultMap.put("apex_store_wins", Long.toString(apexStorageWins));
        resultMap.put("apex_store_lag_avg", String.format("%2.02f", apexStorageAverageLatency));
        resultMap.put("apex_store_lag_sum", Long.toString(apexStorageCumulativeLag));
        resultMap.put("apex_store_speed_pct", String.format("%2.02f", apex_store_speed_pct));
        resultMap.put("spark_streaming_wins", Long.toString(sparkStorageWins));
        resultMap.put("spark_streaming_lag_avg", String.format("%2.02f", sparkAverageLatency));
        resultMap.put("spark_streaming_lag_sum", Long.toString(sparkCumulativeLag));
        resultMap.put("spark_streaming_speed_pct", String.format("%2.02f", spark_streaming_speed_pct));
        snapshotList.add(resultMap);
        snapshotOutput.emit(snapshotList);
        processingMap.remove(tuple.getBatchCount());
      }
    }
  }

  @Override
  public void endWindow()
  {
    processingMap.compact();
    super.endWindow();
  }
}
