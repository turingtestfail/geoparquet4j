package com.themis.geoparquet4j.validate.rules;


import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


import static org.junit.jupiter.api.Assertions.assertTrue;

public class RulesTest {
    @Test
    public void testRequiredGeoKeyRule() throws Exception {
        ParquetFileReader reader = getParquetFileReader();
            RequiredGeoKeyRule rule = new RequiredGeoKeyRule(reader);
            assertTrue(rule.validate());
    }

    private static ParquetFileReader getParquetFileReader() {
        File file = new File("src/test/resources/example-v1.0.0.parquet");
        Path path= Paths.get(file.getAbsolutePath());
        try (
                ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path));
        ) {
            return reader;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testPrimaryColumnLookupRule() throws Exception {
            ParquetFileReader reader = getParquetFileReader();
            PrimaryColumnLookupRule rule = new PrimaryColumnLookupRule(reader);
            assertTrue(rule.validate());
        }

    @Test
    public void testGeometryRepetitionRule() throws Exception {
        ParquetFileReader reader = getParquetFileReader();
        GeometryRepetitionRule rule = new GeometryRepetitionRule(reader);
        assertTrue(rule.validate());
    }
}
