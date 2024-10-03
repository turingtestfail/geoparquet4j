package com.themis.geoparquet4j.validate.rules;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import  org.apache.parquet.io.InputFile;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RulesTest {
    @Test
    public void testRequiredGeoKeyRule() throws Exception {
        File file = new File("src/test/resources/example-v1.0.0.parquet");
        Path path= Paths.get(file.getAbsolutePath());
        try (
                ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path));
        ) {
            RequiredGeoKeyRule rule = new RequiredGeoKeyRule(reader);
            assertTrue(rule.validate("value"));
        }
    }
}
