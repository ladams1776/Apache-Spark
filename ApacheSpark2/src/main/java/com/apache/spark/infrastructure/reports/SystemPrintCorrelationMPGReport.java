package com.apache.spark.infrastructure.reports;

import static java.lang.String.valueOf;

import com.apache.spark.domain.shared.OutcomeCorrelationReport;
import com.apache.spark.infrastructure.reports.filters.EverythingButStringTypesFilter;

import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class SystemPrintCorrelationMPGReport implements
        OutcomeCorrelationReport<StructType, Dataset<Row>> {

    private EverythingButStringTypesFilter filter = new EverythingButStringTypesFilter();

    @Override
    public void apply(StructType autoSchema, Dataset<Row> autoCleansed) {
        Stream.of(autoSchema.fields())
                .filter(field -> this.filter.apply(field))
                .forEach(field -> System.out.println("Correlation between MPG and "
                        .concat(field.name())
                        .concat(" is ")
                        .concat(valueOf(autoCleansed.stat().corr("MPG", field.name()))))
                );
    }
}
