package datahub.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv$;
import scala.Option;

@Slf4j
public class AppNameHelper {
    public static String getAppNameShort(String name) {
        SparkConf config = SparkEnv$.MODULE$.get().conf();
        boolean isFlowNameDefined = config.getOption("spark.datahub.flow_name").isDefined();
        String splitReg = config.getOption("spark.datahub.appname.split_regex").getOrElse(() -> null);
        String appnamePattern = config.getOption("spark.datahub.appname.pattern").getOrElse(() -> null);

        String appShortName;
        if (!isFlowNameDefined && splitReg != null && appnamePattern != null)
            appShortName = name.replaceAll(splitReg.replace("\\\\", "\\"), appnamePattern);
        else
            appShortName = name;

        log.debug("For appname \"{}\" getAppNameShort returned \"{}\"", name, appShortName);
        return appShortName;
    }
}
