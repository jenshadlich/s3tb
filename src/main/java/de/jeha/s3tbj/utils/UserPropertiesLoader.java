package de.jeha.s3tbj.utils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author jenshadlich@googlemail.com
 */
public interface UserPropertiesLoader {

    Properties load(String context) throws IOException;

}
