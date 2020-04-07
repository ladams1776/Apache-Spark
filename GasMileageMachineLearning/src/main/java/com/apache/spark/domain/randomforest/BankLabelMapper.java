package com.apache.spark.domain.randomforest;

public interface BankLabelMapper<T, R> {
  public R map(T row);
}
