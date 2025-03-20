package datahub.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv$;

@Slf4j
public class AppNameHelper {
    public static String getAppNameShort(String name) {
        SparkConf config = SparkEnv$.MODULE$.get().conf();
        String splitReg = config.get("spark.datahub.appname.split_regex");
        String appnamePattern = config.get("spark.datahub.appname.pattern");

        String appShortName = name.replaceAll(splitReg, appnamePattern);

        log.debug("getAppNameShort completed successfully with {}", appShortName);
        return appShortName;
    }
}
