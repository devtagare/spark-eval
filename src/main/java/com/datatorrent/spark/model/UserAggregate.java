package com.datatorrent.spark.model;

import java.io.Serializable;

public class UserAggregate extends UserEvent implements Serializable
{
  private static final long serialVersionUID = 354336765166795320L;
  private Double revenue;
  private Long count;

  public UserAggregate()
  {
  }

  public void copy(UserAggregate userAggregate)
  {
    
    this.setId(userAggregate.getId());
    this.setName(userAggregate.getName());
    this.setPrice(userAggregate.getPrice());
    this.setTags(userAggregate.getTags());
    this.setTime(userAggregate.getTime());
    
  }

  public Double getRevenue()
  {
    return revenue;
  }

  public void setRevenue(Double revenue)
  {
    this.revenue = revenue;
  }

  public Long getCount()
  {
    return count;
  }

  public void setCount(Long count)
  {
    this.count = count;
  }

  @Override
  public String toString()
  {
    return "UserAggregate [getRevenue()=" + getRevenue() + ", getCount()=" + getCount() + ", getId()=" + getId()
        + ", getName()=" + getName() + ", getPrice()=" + getPrice() + ", getTags()=" + getTags() + ", getTime()="
        + getTime() + ", toString()=" + super.toString() + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
        + "]";
  }

}
