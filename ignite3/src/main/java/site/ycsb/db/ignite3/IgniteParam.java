package site.ycsb.db.ignite3;

import java.util.Properties;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

/**
 * Ignite client parameter.
 *
 * @param <T> Parameter value type.
 */
public final class IgniteParam<T> {
  public static final IgniteParam<Boolean> DEBUG =
      new IgniteParam<>("debug", false, Boolean::parseBoolean);

  public static final IgniteParam<Boolean> SHUTDOWN_IGNITE =
      new IgniteParam<>("shutdownIgnite", false, Boolean::parseBoolean);

  public static final IgniteParam<Boolean> USE_EMBEDDED =
      new IgniteParam<>("useEmbedded", false, Boolean::parseBoolean);

  public static final IgniteParam<Boolean> DISABLE_FSYNC =
      new IgniteParam<>("disableFsync", false, Boolean::parseBoolean);

  public static final IgniteParam<Boolean> USE_COLUMNAR =
      new IgniteParam<>("useColumnar", false, Boolean::parseBoolean);

  public static final IgniteParam<String> DB_ENGINE =
      new IgniteParam<>("dbEngine", "", s -> s);

  public static final IgniteParam<String> STORAGE_PROFILES =
      new IgniteParam<>("storage_profiles", "", s -> s);

  public static final IgniteParam<String> SECONDARY_STORAGE_PROFILE =
      new IgniteParam<>("secondary_storage_profile", "", s -> s);

  public static final IgniteParam<String> REPLICAS =
      new IgniteParam<>("replicas", "", s -> s);

  public static final IgniteParam<String> PARTITIONS =
      new IgniteParam<>("partitions", "", s -> s);

  public static final IgniteParam<Boolean> TX_READ_ONLY =
      new IgniteParam<>("txreadonly", false, Boolean::parseBoolean);

  public static final IgniteParam<String> WORK_DIR =
      new IgniteParam<>("workDir", "../ignite3-ycsb-work/" + System.currentTimeMillis(), s -> s);

  /**
   * Parameter name.
   */
  @NotNull
  private String parameter;

  /**
   * Default parameter value.
   */
  @NotNull
  private T defaultValue;

  /**
   * Parsing function to get parameter value from property.
   */
  @NotNull
  private Function<String, T> parser;

  /**
   * Constructor.
   *
   * @param parameter Parameter.
   * @param defaultValue Default value for parameter.
   * @param parser Parsing function to get parameter value from property.
   */
  private IgniteParam(@NotNull String parameter, @NotNull T defaultValue, @NotNull Function<String, T> parser) {
    this.parameter = parameter;
    this.defaultValue = defaultValue;
    this.parser = parser;
  }

  /**
   * Get parameter value from properties.
   *
   * @param properties Properties.
   */
  @NotNull
  public T getValue(@NotNull Properties properties) {
    if (properties.containsKey(parameter)) {
      return parser.apply(properties.getProperty(parameter));
    }

    return defaultValue;
  }
}
