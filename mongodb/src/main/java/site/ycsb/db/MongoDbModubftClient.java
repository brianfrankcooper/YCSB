package site.ycsb.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import org.bson.Document;

import mongo.Mongo.*;
import mongo.Mongo.MongoRequest.RequestType;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

public class MongoDbModubftClient extends DB {
    private int moduBftClientPort = 10000;

    private Socket activeConnection;

    private OutputStream out;
    private InputStream in;

    private int tId;

    private static int batchSize;

    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

    /** The bulk inserts pending for the thread. */
    private final List<Document> bulkInserts = new ArrayList<Document>();

    public void init() throws DBException {
        try {
            INIT_COUNT.incrementAndGet();
            Properties props = getProperties();
            batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
            tId = Integer.parseInt(props.getProperty("threadId"));
            moduBftClientPort += tId;
            activeConnection = new Socket("localhost", moduBftClientPort);
            out = activeConnection.getOutputStream();
            in = activeConnection.getInputStream();
        } catch (Exception e) {
            throw new DBException(
                    String.format("Failed to establish  connection with modubft client at port %d", moduBftClientPort),
                    e);
        }
    }

    /**
     * Delete a record from the database.
     * 
     * @param table
     *              The name of the table
     * @param key
     *              The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See the {@link DB}
     *         class's description for a discussion of error codes.
     */
    @Override
    public Status delete(String table, String key) {

        DeleteRequest.Builder deleteRequestBuilder = DeleteRequest.newBuilder();
        deleteRequestBuilder.setCollection(table);
        deleteRequestBuilder.setId(key);
        DeleteRequest deleteRequest = deleteRequestBuilder.build();

        MongoRequest.Builder requestBuilder = MongoRequest.newBuilder();
        requestBuilder.setType(RequestType.DELETE);
        requestBuilder.setDelete(deleteRequest);
        MongoRequest request = requestBuilder.build();

        MongoResponse response = sendRequest(request);

        if (response.getStatus().equals(mongo.Mongo.MongoResponse.Status.OK)) {
            return Status.OK;
        }

        return Status.ERROR;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table
     *               The name of the table
     * @param key
     *               The record key of the record to insert.
     * @param values
     *               A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See the {@link DB}
     *         class's description for a discussion of error codes.
     */
    @Override
    public Status insert(String table, String key,
            Map<String, ByteIterator> values) {
        Document toInsert = new Document("_id", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            toInsert.put(entry.getKey(), entry.getValue().toArray());
        }
        InsertRequest insertRequest;
        if (batchSize == 1) {
            List<BSONDocument> bsonDocuments = convertDocuments2BSONs(Arrays.asList(toInsert));
            insertRequest = InsertRequest.newBuilder().setCollection(table)
                    .addAllDocuments(bsonDocuments).build();
        } else {
            bulkInserts.add(toInsert);
            if (bulkInserts.size() == batchSize) {
                List<BSONDocument> bsonDocuments = convertDocuments2BSONs(bulkInserts);
                insertRequest = InsertRequest.newBuilder().setCollection(table)
                        .addAllDocuments(bsonDocuments).build();
            } else {
                return Status.BATCHED_OK;
            }
        }

        MongoRequest request = MongoRequest.newBuilder().setType(RequestType.INSERT).setInsert(insertRequest).build();
        MongoResponse response = sendRequest(request);
        if (response.getStatus().equals(mongo.Mongo.MongoResponse.Status.OK)) {
            return Status.OK;
        }

        return Status.ERROR;
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     * 
     * @param table
     *               The name of the table
     * @param key
     *               The record key of the record to read.
     * @param fields
     *               The list of fields to read, or null for all of them
     * @param result
     *               A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public Status read(String table, String key, Set<String> fields,
            Map<String, ByteIterator> result) {
        // build read request
        ReadRequest.Builder readRequesBuilder = ReadRequest.newBuilder();
        readRequesBuilder.setCollection(table);
        if (fields != null) {
            readRequesBuilder.addAllFields(fields);
        }
        readRequesBuilder.setId(key);
        ReadRequest readRequest = readRequesBuilder.build();

        // build mongo request
        MongoRequest.Builder requestBuilder = MongoRequest.newBuilder();
        requestBuilder.setRead(readRequest).setType(RequestType.READ);
        MongoRequest request = requestBuilder.build();

        MongoResponse response = sendRequest(request);

        if (response.getStatus().equals(MongoResponse.Status.OK) && response.getRead().hasDocument()) {
            return Status.OK;
        }
        return Status.ERROR;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a HashMap.
     * 
     * @param table
     *                    The name of the table
     * @param startkey
     *                    The record key of the first record to read.
     * @param recordcount
     *                    The number of records to read
     * @param fields
     *                    The list of fields to read, or null for all of them
     * @param result
     *                    A Vector of HashMaps, where each HashMap is a set
     *                    field/value
     *                    pairs for one record
     * @return Zero on success, a non-zero error code on error. See the {@link DB}
     *         class's description for a discussion of error codes.
     */
    @Override
    public Status scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        ScanRequest.Builder scanRequestBuilder = ScanRequest.newBuilder().setCollection(table).setStartkey(startkey);
        if (fields != null) {
            scanRequestBuilder.addAllFields(fields);
        }
        ScanRequest scanRequest = scanRequestBuilder.build();
        MongoRequest request = MongoRequest.newBuilder().setScan(scanRequest).build();
        MongoResponse response = sendRequest(request);
        if (response.getStatus().equals(MongoResponse.Status.OK)
                && response.getScan().getDocumentsList().size() == recordcount) {
            return Status.OK;
        }
        return Status.ERROR;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *               The name of the table
     * @param key
     *               The record key of the record to write.
     * @param values
     *               A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public Status update(String table, String key,
            Map<String, ByteIterator> values) {
        Document fieldsToSet = new Document();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
        }
        Document update = new Document("$set", fieldsToSet);
        BSONDocument updateBSON = convertDocument2BSON(update);
        UpdateRequest updateRequest = UpdateRequest.newBuilder().setCollection(table).setId(key)
                .setUpdateDocument(updateBSON).build();
        MongoRequest request = MongoRequest.newBuilder().setUpdate(updateRequest).setType(RequestType.UPDATE).build();
        MongoResponse response = sendRequest(request);
        if (response.getStatus().equals(MongoResponse.Status.OK)) {
            return Status.OK;
        }
        return Status.ERROR;
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (INIT_COUNT.decrementAndGet() == 0) {
            CleanupRequest cleanupRequest = CleanupRequest.newBuilder().build();
            MongoRequest request = MongoRequest.newBuilder().setType(RequestType.CLEANUP).setCleanup(cleanupRequest)
                    .build();
            MongoResponse response = sendRequest(request);
            if (!response.getStatus().equals(MongoResponse.Status.OK)) {
                throw new DBException("Cleanup unsuccessful");
            }
        }

    }

    private List<BSONDocument> convertDocuments2BSONs(List<Document> documents) {
        List<BSONDocument> bsonDocuments = new ArrayList<>();
        for (Document document : documents) {
            BSONDocument bsonDocument = convertDocument2BSON(document);
            bsonDocuments.add(bsonDocument);
        }
        return bsonDocuments;
    }

    private BSONDocument convertDocument2BSON(Document document) {
        byte[] documentBytes = BsonToBytes.toBytes(document);
        ByteString documentByteString = ByteString.copyFrom(documentBytes);
        return BSONDocument.newBuilder().setData(documentByteString).build();
    }

    private MongoResponse sendRequest(MongoRequest request) {
        try {
            byte[] requestBytes = request.toByteArray();
            byte[] length = ByteBuffer.allocate(4).putInt(requestBytes.length).array();
            out.write(length);
            out.write(requestBytes);
            out.flush();
            return getResponse();
        } catch (Exception e) {
            e.printStackTrace();
            MongoResponse.Builder badResponseBuilder = MongoResponse.newBuilder();
            badResponseBuilder.setStatus(mongo.Mongo.MongoResponse.Status.ERROR);
            return badResponseBuilder.build();
        }
    }

    private MongoResponse getResponse() throws IOException {
        byte[] lengthBuf = new byte[4];
        int bytesRead = 0;

        while (bytesRead < 4) {
            int result = in.read(lengthBuf, bytesRead, 4 - bytesRead);
            if (result == -1) {
                if (bytesRead == 0) {
                    return null; // end of stream
                } else {
                    throw new IOException("Unexpected end of stream");
                }
            }
            bytesRead += result;
        }
        int length = ByteBuffer.wrap(lengthBuf).getInt();
        if (length == 0) {
            throw new IOException("response size can't be 0");
        }
        byte[] messageBytes = new byte[length];

        bytesRead = 0;

        while (bytesRead < length) {
            int result = in.read(messageBytes, bytesRead, length);
            if (result == -1) {
                throw new IOException("Unexpected end of stream");
            }
            bytesRead += result;
        }
        MongoResponse response = MongoResponse.parseFrom(messageBytes);

        return response;
    }

}
