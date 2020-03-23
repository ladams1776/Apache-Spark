package com.apache.spark.domain.reports;

public interface CorrelationMPGReport<T, E> {
  void report(T schema, E data);
}
