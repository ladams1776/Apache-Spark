package com.apache.spark.domain.randomforest;

public interface OutcomeCorrelationReport<T, B> {

  public void print(T bankSchema, B cleansedBanks);
}
