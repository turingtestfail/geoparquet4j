package com.themis.geoparquet4j.validate.rules;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.Type;


import java.io.IOException;
import java.util.Map;


public class GeometryRepetitionRule extends GenericRule {
    private String message;
    public GeometryRepetitionRule(ParquetFileReader reader) {
        super(reader);
    }

    @Override
    public boolean validate() throws IOException {
        Map<String,String> columns = (Map<String, String>) metadata.get("columns");
        for(String key: columns.keySet()){
            if(!typeIsPresent(key)){
                message = "Column "+key+" not found in schema";
                return false;
            }
            Type type = getType(key);
      if (type != null) {
        if (Type.Repetition.REPEATED==type.getRepetition()) {
            message = "Column "+key+" should not be repeated";
          return false;
        }
        if (Type.Repetition.OPTIONAL != type.getRepetition()&&Type.Repetition.REQUIRED != type.getRepetition()) {
            message = "Column "+key+" should be required or optional";
          return false;
          }
    }else{
          message = "Column "+key+" not found in schema";
          return false;
      }
        }
        return true;
    }

    @Override
    public String getErrorMessage() {
        return message;
    }
}
