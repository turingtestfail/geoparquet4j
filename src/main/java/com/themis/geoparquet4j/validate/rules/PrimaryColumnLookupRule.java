package com.themis.geoparquet4j.validate.rules;

import org.apache.parquet.hadoop.ParquetFileReader;

import java.io.IOException;

import static com.themis.geoparquet4j.validate.rules.GeoParquestConstants.PRIMARY_COLUMN;

public class PrimaryColumnLookupRule extends GenericRule{

    public static final String PRIMARY_COLUMN_LOOKUP_MESSAGE = "missing "+PRIMARY_COLUMN+" in metadata";

    public PrimaryColumnLookupRule(ParquetFileReader reader) {
        super(reader);
    }

    @Override
    public boolean validate() throws IOException {
        return metadata.containsKey(PRIMARY_COLUMN);
    }

    @Override
    public String getErrorMessage() {
        return PRIMARY_COLUMN_LOOKUP_MESSAGE;
    }
}
