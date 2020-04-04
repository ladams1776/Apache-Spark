package com.apache.spark.domain.randomforest;

public interface BankIndicatorMapper<T> {

  public T map(T row) throws Exception;
}
