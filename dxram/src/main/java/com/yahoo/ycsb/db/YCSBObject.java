package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Data structure for YCSB objects.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.04.2017
 */
public class YCSBObject extends AbstractChunk {
  private final int fieldSize;
  private final byte[] data;
  private final Iterator[] fieldIterators;

  YCSBObject(final int numFields, final int fieldSize) {
    super();

    this.fieldSize = fieldSize;
    data = new byte[numFields * fieldSize];
    fieldIterators = new Iterator[numFields];

    for (int i = 0; i < fieldIterators.length; i++) {
      fieldIterators[i] = new Iterator(i * fieldSize);
    }
  }

  // -----------------------------------------------------------------------------

  Iterator getFieldIterator(final String fieldKey) {
    int idx = Integer.parseInt(fieldKey.substring(5));

    fieldIterators[idx].reset();
    return fieldIterators[idx];
  }

  Iterator getFieldIterator(final int fieldIdx) {
    fieldIterators[fieldIdx].reset();
    return fieldIterators[fieldIdx];
  }

  void setFieldValue(final String fieldKey, final ByteIterator it) {
    int idx = Integer.parseInt(fieldKey.substring(5));
    int offset = idx * fieldSize;
    int i = 0;

    while (it.hasNext()) {
      data[offset + i++] = it.nextByte();
    }
  }

  // -----------------------------------------------------------------------------

  @Override
  public void exportObject(final Exporter exporter) {
    exporter.writeBytes(data);
  }

  @Override
  public void importObject(final Importer importer) {
    importer.readBytes(data);
  }

  @Override
  public int sizeofObject() {
    return data.length;
  }

  /**
   * Byte iterator for data of YCSBObject.
   */
  public class Iterator extends ByteIterator {
    private final int startOffset;
    private int iteratorPos;

    public Iterator(final int startOffset) {
      this.startOffset = startOffset;
    }

    public void reset() {
      iteratorPos = startOffset;
    }

    @Override
    public boolean hasNext() {
      return iteratorPos - startOffset < fieldSize;
    }

    @Override
    public byte nextByte() {
      return data[startOffset + iteratorPos++];
    }

    @Override
    public long bytesLeft() {
      return fieldSize - (iteratorPos - startOffset);
    }
  }
}
