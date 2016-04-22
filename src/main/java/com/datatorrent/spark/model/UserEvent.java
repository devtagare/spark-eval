package com.datatorrent.spark.model;

import java.io.Serializable;
import java.util.ArrayList;

public class UserEvent implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 6706192712663517404L;
  private Long id;
  private String name;
  private Double price;
  private ArrayList<String> tags;
  private Long time;
  
  

  public UserEvent(){
    
  }
  
  public Long getId()
  {
    return id;
  }

  public void setId(Long id)
  {
    this.id = id;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public Double getPrice()
  {
    return price;
  }

  public void setPrice(Double price)
  {
    this.price = price;
  }

  public ArrayList<String> getTags()
  {
    return tags;
  }

  public void setTags(ArrayList<String> tags)
  {
    this.tags = tags;
  }

  public Long getTime()
  {
    return time;
  }

  public void setTime(Long time)
  {
    this.time = time;
  }
  
  @Override
  public String toString()
  {
    return "UserEvent [id=" + id + ", name=" + name + ", price=" + price + ", tags=" + tags + ", time=" + time + "]";
  }

}
