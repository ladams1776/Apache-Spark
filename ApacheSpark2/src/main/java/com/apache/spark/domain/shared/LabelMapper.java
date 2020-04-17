package com.apache.spark.domain.shared;

public interface LabelMapper<T, R> {
  public R map(T row);
}
