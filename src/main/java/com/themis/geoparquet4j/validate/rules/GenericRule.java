package com.themis.geoparquet4j.validate.rules;



import org.apache.parquet.hadoop.ParquetFileReader;

import java.io.IOException;

public abstract class GenericRule implements Rule {
    protected final ParquetFileReader reader;
    public GenericRule(ParquetFileReader reader) {
        this.reader = reader;

    }
    public abstract boolean validate(String value) throws IOException;

    public abstract String getErrorMessage();
}
