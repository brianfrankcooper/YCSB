package site.ycsb.db.ignite3;

import java.util.Properties;
import java.util.function.Function;

/**
 * Ignite client parameter.
 *
 * @param <T> Parameter value type.
 */
public class IgniteParam<T> {
  public static final IgniteParam<Boolean> DEBUG = new IgniteParam<>("debug", false,
      Boolean::parseBoolean);

  public static final IgniteParam<Boolean> USE_EMBEDDED = new IgniteParam<>("useEmbedded", false,
      Boolean::parseBoolean);

  public static final IgniteParam<Boolean> DISABLE_FSYNC = new IgniteParam<>("disableFsync", false,
      Boolean::parseBoolean);

  public static final IgniteParam<String> DB_ENGINE = new IgniteParam<>("dbEngine", "", s -> s);

  public static final IgniteParam<String> STORAGE_PROFILES = new IgniteParam<>("storage_profiles", "", s -> s);

  public static final IgniteParam<String> REPLICAS = new IgniteParam<>("replicas", "", s -> s);

  public static final IgniteParam<String> PARTITIONS = new IgniteParam<>("partitions", "", s -> s);

  public static final IgniteParam<String> WORK_DIR = new IgniteParam<>("workDir",
      "../ignite3-ycsb-work/" + System.currentTimeMillis(), s -> s);

  /**
   * Parameter name.
   */
  String parameter;

  /**
   * Default parameter value.
   */
  T defaultValue;

  /**
   * Parsing function to get parameter value from property.
   */
  Function<String, T> parser;

  private IgniteParam(String parameter, T defaultValue, Function<String, T> parser) {
    this.parameter = parameter;
    this.defaultValue = defaultValue;
    this.parser = parser;
  }

  public T getValue(Properties properties) {
    if (properties.containsKey(parameter)) {
      return parser.apply(properties.getProperty(parameter));
    }

    return defaultValue;
  }
}
