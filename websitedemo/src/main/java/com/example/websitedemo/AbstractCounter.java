package com.example.websitedemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

@Stateless
public abstract class AbstractCounter extends BaseOperator
{
  private transient long counterInterval;
  private transient long currentIntervalTime;
  private long batchSize = 500;
  private long batchesCounted = 0;
  private transient long intervalStartTime;
  private transient boolean resetInterval = true;

  public static final Logger LOG = LoggerFactory.getLogger(AbstractCounter.class);

  public long getIntervalStartTime()
  {
    return intervalStartTime;
  }

  public void setIntervalStartTime(long intervalStartTime)
  {
    this.intervalStartTime = intervalStartTime;
  }

  public long getCurrentIntervalTime()
  {
    return currentIntervalTime;
  }

  public void setCurrentIntervalTime(long currentIntervalTime)
  {
    this.currentIntervalTime = currentIntervalTime;
  }

  public long getCounterInterval()
  {
    return counterInterval;
  }

  public void setCounterInterval(long counterInterval)
  {
    this.counterInterval = counterInterval;
  }

  public long getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(long batchSize)
  {
    this.batchSize = batchSize;
  }

  public long getBatchesCounted()
  {
    return batchesCounted;
  }

  public void setBatchesCounted(long batchesCounted)
  {
    this.batchesCounted = batchesCounted;
  }

  public abstract void emitEvent();

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

  public transient DefaultOutputPort<String> kafkaOut = new DefaultOutputPort<String>();

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      if (resetInterval) {
        resetInterval = false;
        counterInterval = 0;
        intervalStartTime = System.currentTimeMillis();
       // if (LOG.isDebugEnabled()) {
          LOG.info("Start time for both Apex apps in AbstractCounter is {}", intervalStartTime);
       // }
      }
      counterInterval++;
      if (counterInterval == batchSize) {
        emitEvent();
        batchesCounted++;
        resetInterval = true;
      }
    }
  };

}
