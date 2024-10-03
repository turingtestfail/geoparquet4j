package com.themis.geoparquet4j.validate.rules;

import java.io.IOException;

public interface Rule {
    boolean validate(String value) throws IOException;
    String getErrorMessage();
}
