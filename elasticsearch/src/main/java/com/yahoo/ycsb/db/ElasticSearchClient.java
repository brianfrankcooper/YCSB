package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.search.SearchHit;

/**
 * ElasticSearch client for YCSB framework.
 *
 * <p>Default properties to set:</p> <ul> <li>es.cluster.name = es.ycsb.cluster
 * <li>es.client = true <li>es.index.key = es.ycsb</ul>
 *
 * @author Sharmarke Aden
 *
 */
public class ElasticSearchClient extends DB {

    public static final String DEFAULT_CLUSTER_NAME = "es.ycsb.cluster";
    public static final String DEFAULT_INDEX_KEY = "es.ycsb";
    private Node node;
    private Client client;
    private String indexKey;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        // initialize OrientDB driver
        Properties props = getProperties();
        this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);
        String clusterName = props.getProperty("cluster.name", DEFAULT_CLUSTER_NAME);
        Boolean newdb = Boolean.parseBoolean(props.getProperty("elasticsearch.newdb", "false"));
        Builder settings = settingsBuilder()
                .put("node.local", "true")
                .put("path.data", System.getProperty("java.io.tmpdir") + "/esdata")
                .put("discovery.zen.ping.multicast.enabled", "false")
                .put("index.mapping._id.indexed", "true")
                .put("index.gateway.type", "none")
                .put("gateway.type", "none")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0");


        //if properties file contains elasticsearch user defined properties
        //add it to the settings file (will overwrite the defaults).
        settings.put(props);
        System.out.println("ElasticSearch starting node = " + settings.get("cluster.name"));
        System.out.println("ElasticSearch node data path = " + settings.get("path.data"));

        node = nodeBuilder().clusterName(clusterName).settings(settings).node();
        node.start();
        client = node.client();

        if (newdb) {
            client.admin().indices().prepareDelete(indexKey).execute().actionGet();
            client.admin().indices().prepareCreate(indexKey).execute().actionGet();
        } else {
            boolean exists = client.admin().indices().exists(Requests.indicesExistsRequest(indexKey)).actionGet().isExists();
            if (!exists) {
                client.admin().indices().prepareCreate(indexKey).execute().actionGet();
            }
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (!node.isClosed()) {
            client.close();
            node.stop();
            node.close();
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            final XContentBuilder doc = jsonBuilder().startObject();

            for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
                doc.field(entry.getKey(), entry.getValue());
            }

            doc.endObject();

            client.prepareIndex(indexKey, table, key)
                    .setSource(doc)
                    .execute()
                    .actionGet();

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        try {
            client.prepareDelete(indexKey, table, key)
                    .execute()
                    .actionGet();
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            final GetResponse response = client.prepareGet(indexKey, table, key)
                    .execute()
                    .actionGet();

            if (response.isExists()) {
                if (fields != null) {
                    for (String field : fields) {
                        result.put(field, new StringByteIterator((String) response.getSource().get(field)));
                    }
                } else {
                    for (String field : response.getSource().keySet()) {
                        result.put(field, new StringByteIterator((String) response.getSource().get(field)));
                    }
                }
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            final GetResponse response = client.prepareGet(indexKey, table, key)
                    .execute()
                    .actionGet();

            if (response.isExists()) {
                for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
                    response.getSource().put(entry.getKey(), entry.getValue());
                }

                client.prepareIndex(indexKey, table, key)
                        .setSource(response.getSource())
                        .execute()
                        .actionGet();

                return 0;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            final RangeFilterBuilder filter = rangeFilter("_id").gte(startkey);
            final SearchResponse response = client.prepareSearch(indexKey)
                    .setTypes(table)
                    .setQuery(matchAllQuery())
                    .setFilter(filter)
                    .setSize(recordcount)
                    .execute()
                    .actionGet();

            HashMap<String, ByteIterator> entry;

            for (SearchHit hit : response.getHits()) {
                entry = new HashMap<String, ByteIterator>(fields.size());

                for (String field : fields) {
                    entry.put(field, new StringByteIterator((String) hit.getSource().get(field)));
                }

                result.add(entry);
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}
