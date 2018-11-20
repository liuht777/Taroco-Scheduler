package cn.taroco.scheduler.core;

/**
 * @author juny.ye
 */
public class Version {

    private final static String VERSION = "uncode-schedule-1.0.0";

    public static String getVersion() {
        return VERSION;
    }

    public static boolean isCompatible(String dataVersion) {
        return VERSION.compareTo(dataVersion) >= 0;
    }

}
