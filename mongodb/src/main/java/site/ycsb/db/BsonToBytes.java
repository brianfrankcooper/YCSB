package site.ycsb.db;

import java.nio.ByteBuffer;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public class BsonToBytes {

    private static final Codec<Document> DOCUMENT_CODEC = new DocumentCodec();

    public static byte[] toBytes(Document document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return buffer.toByteArray();
    }

    public static Document toDocument(byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        Document document = DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        reader.close();
        return document;
    }
}
