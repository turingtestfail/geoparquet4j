package com.themis.geoparquet4j.validate.rules;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.Map;

public class RequiredGeoKeyRule extends GenericRule {

    public static final String REQUIRED_GEOKEY_MESSAGE = "File must include " + GeoParquestConstants.MetadataKey + " metadata key";

    public RequiredGeoKeyRule(ParquetFileReader reader) {
        super(reader);
    }

    @Override
    public boolean validate() throws IOException {
        ParquetMetadata metadata = reader.getFooter();
        FileMetaData fileMetaData = metadata.getFileMetaData();
        return fileMetaData.getKeyValueMetaData().containsKey(GeoParquestConstants.MetadataKey);
    }

    @Override
    public String getErrorMessage() {
        return REQUIRED_GEOKEY_MESSAGE;
    }
}
