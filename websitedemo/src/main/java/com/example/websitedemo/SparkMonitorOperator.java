package com.example.websitedemo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class SparkMonitorOperator extends BaseOperator
{

  public static final DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {



    }
  };


}
