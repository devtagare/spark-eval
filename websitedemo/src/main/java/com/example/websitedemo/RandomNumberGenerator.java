/**
 * Put your copyright and license info here.
 */
package com.example.websitedemo;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 10;
  private transient int count = 0;

  public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count == numTuples) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException ie) {
        ie.printStackTrace();
      }
    }
    count++;
    out.emit("" + System.currentTimeMillis());

  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   *
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}
