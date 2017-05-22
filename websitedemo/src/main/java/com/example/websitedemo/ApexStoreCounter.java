package com.example.websitedemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.Stateless;

@Stateless
public class ApexStoreCounter extends AbstractCounter
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexStoreCounter.class);

  @Override
  public void emitEvent()
  {
    Event kafkaEvent = new Event();
    kafkaEvent.setInitialTimeStamp(getIntervalStartTime());
    kafkaEvent.setTypeOfApp(1);
    kafkaEvent.setBatchCount(getBatchesCounted());
    //if (LOG.isDebugEnabled()) {
      LOG.debug("Before : Time stamp for the event with timestamp {} and # count {} before emitting to Kafka : {}", getIntervalStartTime(), getBatchesCounted(), System.currentTimeMillis());
   // }
    kafkaOut.emit(kafkaEvent.toString());
   // if (LOG.isDebugEnabled()) {
      LOG.debug("After : Time stamp for the event with timestamp {} and # count {} after emitting to Kafka : {}", getIntervalStartTime(), getBatchesCounted(), System.currentTimeMillis());
   // }
  }


}
