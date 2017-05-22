package com.example.websitedemo;

public class Event
{
  private long initialTimeStamp;
  private long finalTimeStamp;
  //factor of 10
  private long batchCount;
  private int typeOfApp;

  public Event()
  {

  }

  public long getInitialTimeStamp()
  {
    return initialTimeStamp;
  }

  public void setInitialTimeStamp(long initialTimeStamp)
  {
    this.initialTimeStamp = initialTimeStamp;
  }

  public long getFinalTimeStamp()
  {
    return finalTimeStamp;
  }

  public void setFinalTimeStamp(long finalTimeStamp)
  {
    this.finalTimeStamp = finalTimeStamp;
  }

  public int getTypeOfApp()
  {
    return typeOfApp;
  }

  public void setTypeOfApp(int typeOfApp)
  {
    this.typeOfApp = typeOfApp;
  }

  public long getBatchCount()
  {
    return batchCount;
  }

  public void setBatchCount(long batchCount)
  {
    this.batchCount = batchCount;
  }

  @Override
  public String toString()
  {
    return
      typeOfApp + "," + initialTimeStamp + "," + batchCount + "," + finalTimeStamp;
  }
}
