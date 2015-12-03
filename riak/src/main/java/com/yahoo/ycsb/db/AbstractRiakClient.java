/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.ycsb.db;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public abstract class AbstractRiakClient extends DB {

    protected static class Config {
        private static final String HOST_PROPERTY = "riak.hosts";
        private static final String PORT_PROPERTY = "riak.port";
        private static final String BUCKET_TYPE_PROPERTY = "riak.bucket_type";
        private static final String R_VALUE_PROPERTY = "riak.r_val";
        private static final String W_VALUE_PROPERTY = "riak.w_val";
        private static final String READ_RETRY_COUNT_PROPERTY = "riak.read_retry_count";

        private String bucketType;
        private int defaultPort;
        private String hosts;
        private int r_value;
        private int w_value;
        private int readRetry;

        private Config() {}

        public int getReadRetry() {
            return readRetry;
        }

        public static Config create(Properties props) {
            final Config cfg = new Config();

            cfg.defaultPort = Integer.parseInt(
                    props.getProperty(PORT_PROPERTY, Integer.toString(RiakNode.Builder.DEFAULT_REMOTE_PORT))
                );

            cfg.hosts = props.getProperty(HOST_PROPERTY, RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);

            cfg.bucketType = props.getProperty(BUCKET_TYPE_PROPERTY, "ycsb");

            cfg.r_value = Integer.parseInt(
                    props.getProperty(R_VALUE_PROPERTY, "2")
                );

            cfg.w_value = Integer.parseInt(
                    props.getProperty(W_VALUE_PROPERTY, "2")
            );

            cfg.readRetry = Integer.parseInt(
                    props.getProperty(READ_RETRY_COUNT_PROPERTY, "5")
            );

            return cfg;
        }

        public Namespace mkNamespaceFor(String table) {
            return new Namespace(bucketType, table);
        }

        public Location mkLocationFor(String table, String key) {
            return new Location(mkNamespaceFor(table), key);
        }

        public Quorum readQuorum() {
            return new Quorum(r_value);
        }

        public Quorum writeQuorum() {
            return new Quorum(w_value);
        }
    }

    private Config config;
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected RiakClient riakClient;


    protected Config config() {
        assert config != null;
        return config;
    }

    @Override
    public void init() throws DBException {
        super.init();

        config = Config.create(getProperties());

        if (logger.isDebugEnabled()) {
            logger.debug("Configuration is:\n" +
                        "\tHosts:        {}\n" +
                        "\tDefault Port: {}\n" +
                        "\tBucket type:  {}\n" +
                        "\tR Val:        {}\n" +
                        "\tW Val:        {}\n" +
                        "\tRead Retry Count: {}",
                    new Object[] {
                        config.hosts,
                        config.defaultPort,
                        config.bucketType,
                        config.r_value,
                        config.w_value,
                        config.readRetry
                    }
            );
        }

        final RiakNode.Builder builder = new RiakNode.Builder();

        try {
            final RiakCluster riakCluster = new RiakCluster.Builder(builder, config.defaultPort, config.hosts).build();
            riakCluster.start();

            riakClient = new RiakClient(riakCluster);
        } catch (UnknownHostException e) {
            throw new DBException("Can't create Riak Cluster", e);
        }
    }

    @Override
    public void cleanup() throws DBException
    {
        if (riakClient != null) {
            riakClient.shutdown();
        }

        if (config != null) {
            config = null;
        }
    }
}
