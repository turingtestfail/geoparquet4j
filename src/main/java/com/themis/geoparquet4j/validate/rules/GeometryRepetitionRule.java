package com.themis.geoparquet4j.validate.rules;

import org.apache.parquet.hadoop.ParquetFileReader;


import java.io.IOException;
import java.util.Map;


public class GeometryRepetitionRule extends GenericRule {
    public GeometryRepetitionRule(ParquetFileReader reader) {
        super(reader);
    }

    @Override
    public boolean validate() throws IOException {
        Map<String,String> columns = (Map<String, String>) metadata.get("columns");
        for(String key: columns.keySet()){
            if(!typeIsPresent(key)){
                return false;
            }
        }
        return true;
    }

    @Override
    public String getErrorMessage() {
        return "";
    }
}
