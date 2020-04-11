package com.apache.spark.domain.randomforest;

public interface BankVariableIndicatorMapper<T, D> {

  public T map(T row) throws Exception;
  public D getSchema();
}
