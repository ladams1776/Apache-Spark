package com.apache.spark.domain.shared;

import java.io.Serializable;

public interface VariableIndicatorMapper<T, D> extends Serializable {

  public T map(T row) throws Exception;
  public D getSchema();
}
