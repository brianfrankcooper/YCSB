package com.yahoo.ycsb.db;

import com.axibase.tsd.client.*;
import com.axibase.tsd.model.data.TimeFormat;
import com.axibase.tsd.model.data.command.GetSeriesQuery;
import com.axibase.tsd.model.data.command.SimpleAggregateMatcher;
import com.axibase.tsd.model.data.series.*;
import com.axibase.tsd.model.data.series.aggregate.AggregateType;
import com.axibase.tsd.model.system.ClientConfiguration;
import com.axibase.tsd.model.system.TcpClientConfiguration;
import com.axibase.tsd.network.SimpleCommand;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;

import javax.ws.rs.core.MultivaluedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ATSD client for YCSB framework.
 */
public class ATSDClient extends TimeseriesDB {

  private static final String HTTP_PORT_PROPERTY = "httpPort";
  private static final String HTTP_PORT_PROPERTY_DEFAULT = "8088";
  private static final String TCP_PORT_PROPERTY = "tcpPort";
  private static final String TCP_PORT_PROPERTY_DEFAULT = "8081";
  private static final String IP_PROPERTY = "ip";
  private static final String IP_PROPERTY_DEFAULT = "localhost";
  private static final String USERNAME_PROPERTY = "username";
  private static final String USERNAME_PROPERTY_DEFAULT = "admin";
  private static final String PASSWORD_PROPERTY = "password";
  private static final String PASSWORD_PROPERTY_DEFAULT = "adminadmin";

  private String ip;
  private int httpPort;
  private int tcpPort;
  private String username;
  private String password;

  private HttpClientManager httpClientManager;
  private TcpClientManager tcpClientManager;
  private DataService dataService;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    try {
      if (!getProperties().containsKey(HTTP_PORT_PROPERTY) && !test) {
        throw new DBException("No http port given, abort.");
      }
      if (!getProperties().containsKey(TCP_PORT_PROPERTY) && !test) {
        throw new DBException("No tcp port given, abort.");
      }
      if (!getProperties().containsKey(IP_PROPERTY) && !test) {
        throw new DBException("No ip given, abort.");
      }
      if (!getProperties().containsKey(USERNAME_PROPERTY) && !test) {
        throw new DBException("No username given, abort.");
      }
      if (!getProperties().containsKey(PASSWORD_PROPERTY) && !test) {
        throw new DBException("No password given, abort.");
      }

      httpPort = Integer.parseInt(getProperties().getProperty(HTTP_PORT_PROPERTY, HTTP_PORT_PROPERTY_DEFAULT));
      tcpPort = Integer.parseInt(getProperties().getProperty(TCP_PORT_PROPERTY, TCP_PORT_PROPERTY_DEFAULT));
      username = getProperties().getProperty(USERNAME_PROPERTY, USERNAME_PROPERTY_DEFAULT);
      ip = getProperties().getProperty(IP_PROPERTY, IP_PROPERTY_DEFAULT);
      password = getProperties().getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);
      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }

    } catch (Exception e) {
      throw new DBException(e);
    }

    httpClientManager = createHttpClientManager(ip, httpPort, username, password);
    dataService = new DataService(httpClientManager);
    tcpClientManager = createTcpClientManager(ip, tcpPort);
  }

  private static HttpClientManager createHttpClientManager(String ip, int httpPort, String username, String passwd) {
    ClientConfigurationFactory configurationFactory = new ClientConfigurationFactory("http", ip, httpPort, // serverPort
        "/api/v1", "/api/v1", username, passwd, 3000, // connectTimeoutMillis
        3000, // readTimeoutMillis
        600000, // pingTimeout
        false, // ignoreSSLErrors
        false, // skipStreamingControl
        false // enableGzipCompression
    );
    ClientConfiguration clientConfiguration = configurationFactory.createClientConfiguration();
    System.out.println("Connecting to ATSD: " + clientConfiguration.getMetadataUrl());
    HttpClientManager httpClientManager = new HttpClientManager(clientConfiguration);
    httpClientManager.setBorrowMaxWaitMillis(1000);

    return httpClientManager;
  }

  private static TcpClientManager createTcpClientManager(String ip, int tcpPort) {
    TcpClientConfigurationFactory tcpConfigurationFactory =
        new TcpClientConfigurationFactory(ip, tcpPort, false, 3000, 10000);
    TcpClientConfiguration tcpClientConfiguration = tcpConfigurationFactory.createClientConfiguration();
    TcpClientManager tcpClientManager = new TcpClientManager();
    tcpClientManager.setClientConfiguration(tcpClientConfiguration);
    tcpClientManager.setBorrowMaxWaitMillis(1000);

    return tcpClientManager;
  }

  public void cleanup() throws DBException {
    httpClientManager.close();
    tcpClientManager.close();
  }


  /**
   * @inheritdoc
   */
  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    if (test) {
      return Status.OK;
    }

    // Problem: You cant ask for a timestamp at TS=x, you need to give a
    // range. So: Begin: timestamp, End: timestamp + 1 ms
    // We may get more than that, but we just take the right one
    // There could also be more of them, so count
    GetSeriesQuery command = new GetSeriesQuery(metric, metric);
    command.setTags(intoMultiMap(tags));
    command.setTimeFormat(TimeFormat.MILLISECONDS);
    command.setStartTime(timestamp);
    command.setEndTime(timestamp + 1);

    List<Series> seriesList = dataService.retrieveSeries(command);

    if (debug) {
      System.out.println("Command: " + command.toString());
      for (Series res : seriesList) {
        System.out.println("Result: " + res.toString());
      }
    }

    if (seriesList == null || seriesList.isEmpty()) {
      System.err.println(
          "ERROR: Found no series for metric: " + metric + " for timestamp: " + timestamp + " to read.");

      return Status.NOT_FOUND;
    }
    int count = 0;
    for (Series series : seriesList) {
      for (Sample sample : series.getData()) {
        if (sample.getTimeMillis().equals(timestamp)) {
          count++;
        }
      }
    }

    if (count == 0) {
      System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
      return Status.NOT_FOUND;
    } else if (count > 1) {
      System.err.println(
          "ERROR: Found more than one value for metric: " + metric + " for timestamp: " + timestamp + ".");
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    if (test) {
      return Status.OK;
    }

    GetSeriesQuery command = new GetSeriesQuery(metric, metric);
    command.setTags(intoMultiMap(tags));
    command.setTimeFormat(TimeFormat.MILLISECONDS);
    command.setStartTime(startTs);
    command.setEndTime(endTs);

    if (aggreg != AggregationOperation.NONE) {
      // AVG;SUM;COUNT
      AggregateType aggregateType = AggregateType.DETAIL;
      switch (aggreg) {
      case AVERAGE:
        aggregateType = AggregateType.AVG;
        break;
      case COUNT:
        aggregateType = AggregateType.COUNT;
        break;
      case SUM:
        aggregateType = AggregateType.SUM;
        break;
      default:
        // keeps the analyzer happy
        break;
      }

      IntervalUnit intervalUnit = IntervalUnit.SECOND; // smallest unit
      if (timeUnit == TimeUnit.MINUTES) {
        intervalUnit = IntervalUnit.MINUTE;
      } else if (timeUnit == TimeUnit.HOURS) {
        intervalUnit = IntervalUnit.HOUR;
      } else if (timeUnit == TimeUnit.DAYS) {
        intervalUnit = IntervalUnit.DAY;
      }

      command.setAggregateMatcher(
          new SimpleAggregateMatcher(new Interval(timeValue, intervalUnit), Interpolate.NONE, aggregateType));
    }

    List<Series> seriesList = dataService.retrieveSeries(command);
    if (debug) {
      System.out.println("Command: " + command.toString());
      for (Series res : seriesList) {
        System.out.println("Result: " + res.toString());
      }
    }
    return Status.OK;
  }

  private MultivaluedHashMap<String, String> intoMultiMap(Map<String, List<String>> tags) {
    MultivaluedHashMap<String, String> tagsMap = new MultivaluedHashMap<String, String>();

    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      String tagKey = entry.getKey();
      for (String tagValue : entry.getValue()) {
        tagsMap.add(tagKey, tagValue);
      }
    }
    return tagsMap;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    SimpleCommand command = new SimpleCommand(
        String.format("series e:%s m:%s=%s ms:%s %s\n",
            metric, metric, value, timestamp, convertTags(tags)));

    tcpClientManager.send(command);

    return Status.OK;
  }

  /**
   * Not implemented for series data, only for entities and metrics.
   */
  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private String convertTags(Map<String, ByteIterator> tags) {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
      builder.append(String.format("t:%s=%s ", entry.getKey(), entry.getValue().toString()));
    }
    return builder.toString();
  }
}
