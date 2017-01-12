package uk.co.onsdigital.discovery.gendata;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.concurrent.TimeUnit;

@Value @Builder
class CsvRecord {
    @NonNull String datasetID;
    @NonNull String s3URL;
    long index;
    @NonNull String row;
    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
}
