package com.themis.geoparquet4j.validate.rules;

import java.io.IOException;
import java.util.Map;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class GeometryDataTypeRule extends GenericRule {
    private String message;
    public GeometryDataTypeRule(ParquetFileReader reader) {
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
       if(!(type instanceof PrimitiveType)){
           message = "Column "+key+" should be of primitive type";
           return false;
       }
       PrimitiveType.PrimitiveTypeName typeName = ((PrimitiveType)type).getPrimitiveTypeName();
         if(!typeName.equals(PrimitiveType.PrimitiveTypeName.BINARY)){
              message = "Column "+key+" should be of BINARY type, instead it is "+typeName.name();
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
