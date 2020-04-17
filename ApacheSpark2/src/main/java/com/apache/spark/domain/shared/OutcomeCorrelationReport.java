package com.apache.spark.domain.shared;

public interface OutcomeCorrelationReport<T, E> {

  void apply(T schema, E data);
}
