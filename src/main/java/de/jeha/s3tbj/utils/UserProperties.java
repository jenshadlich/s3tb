package de.jeha.s3tbj.utils;

/**
 * @author jenshadlich@googlemail.com
 */
public class UserProperties {

    public static UserPropertiesLoader fromHome() {
        return new UserHomePropertiesLoader();
    }

}
