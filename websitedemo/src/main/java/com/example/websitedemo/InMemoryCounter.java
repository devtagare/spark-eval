package com.example.websitedemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.Stateless;

@Stateless
public class InMemoryCounter extends AbstractCounter
{
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryCounter.class);
  @Override
  public void emitEvent()
  {
    Event event = new Event();
    event.setInitialTimeStamp(getIntervalStartTime());
    LOG.info("Timestamp when in memory is complete is : {}",System.currentTimeMillis());
    event.setFinalTimeStamp(System.currentTimeMillis());
    event.setTypeOfApp(0);
    event.setBatchCount(getBatchesCounted());
    output.emit((Object)event);
  }
}
