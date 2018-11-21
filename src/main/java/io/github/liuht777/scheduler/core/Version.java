package io.github.liuht777.scheduler.core;

/**
 * @author liuht
 */
public class Version {

    private final static String VERSION = "taroco-scheduler-1.0.0";

    public static String getVersion() {
        return VERSION;
    }

    public static boolean isCompatible(String dataVersion) {
        return VERSION.compareTo(dataVersion) >= 0;
    }

}
