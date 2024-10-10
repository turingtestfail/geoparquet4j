package com.themis.geoparquet4j.validate.rules;



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.Type;

import javax.lang.model.type.PrimitiveType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public abstract class GenericRule {
    protected final ParquetFileReader reader;
    protected final HashMap<String, Object> metadata;
    protected final List<Type> types;
    public GenericRule(ParquetFileReader reader) {
        this.reader = reader;
        String jsonString = reader.getFileMetaData().getKeyValueMetaData().get(GeoParquestConstants.MetadataKey);
        if(jsonString == null){
            throw new IllegalArgumentException("GeoParquet metadata key not found");
        }
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String,Object>> typeRef
                = new TypeReference<HashMap<String,Object>>() {};

        try {
            metadata = mapper.readValue(jsonString, typeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        types = reader.getFileMetaData().getSchema().getFields();
    }
    public boolean typeIsPresent(String type){
        for(Type t: types){
            if(t.getName().equals(type)){
                return true;
            }
        }
        return false;
    }
    public Type getType(String type){
        for(Type t: types){
            if(t.getName().equals(type)){
                return t;
            }
        }
        return null;
    }
    public abstract boolean validate() throws IOException;

    public abstract String getErrorMessage();
}
