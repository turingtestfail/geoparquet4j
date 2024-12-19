package com.themis.geoparquet4j.validate.rules;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.Map;

public class OptionalEpochRule extends GenericRule{
    private String message;

    public OptionalEpochRule(ParquetFileReader reader) {
        super(reader);
    }

    @Override
    public boolean validate() throws IOException {
        Type type = getType("epoch");
        if(type!=null){
            if(!(type instanceof PrimitiveType)){
                message = "Column epoch should be of primitive type";
                return false;
            }
            PrimitiveType.PrimitiveTypeName typeName = ((PrimitiveType)type).getPrimitiveTypeName();
            if(!typeName.equals(PrimitiveType.PrimitiveTypeName.INT64)){
                message = "Column epoch should be of INT64 type, instead it is "+typeName.name();
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
