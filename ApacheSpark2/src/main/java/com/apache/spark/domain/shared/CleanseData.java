package com.apache.spark.domain.shared;

import java.io.Serializable;

public interface CleanseData<T> extends Serializable {
    public T apply(T data);
}
