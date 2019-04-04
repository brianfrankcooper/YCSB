package com.yahoo.ycsb.db;

import java.util.List;

import de.hhu.bsinfo.dxmem.data.ChunkID;

/**
 * Helper class to convert YCSB keys to DXRAM chunk IDs.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.04.2017
 */
class ChunkIDConverter {
  // skip chunk 0 on every node which is used to store the nameservice entries
  private static  final long CHUNK_ID_OFFSET = 1;

  private final List<Short> storageNodes;
  private final int recordsPerNode;
  private final DXRAMProperties.DistributionStrategy distributionStrategy;

  ChunkIDConverter(final List<Short> storageNodes, final int totalRecords,
                   final DXRAMProperties.DistributionStrategy distributionStrategy) {
    this.storageNodes = storageNodes;
    this.recordsPerNode = totalRecords / storageNodes.size() + 1;
    this.distributionStrategy = distributionStrategy;
  }

  long toChunkId(final String key) {
    // key is of format: userX, e.g. user1, user12, etc.
    int keyVal = Integer.parseInt(key.substring(4));

    if(distributionStrategy == DXRAMProperties.DistributionStrategy.LINEAR) {
      int nodeIdx = keyVal / recordsPerNode;
      int recordIdx = keyVal % recordsPerNode;

      return ChunkID.getChunkID(storageNodes.get(nodeIdx), recordIdx + CHUNK_ID_OFFSET);
    } else {
      int nodeIdx = keyVal % storageNodes.size();
      int recordIdx = keyVal / storageNodes.size();

      return ChunkID.getChunkID(storageNodes.get(nodeIdx), recordIdx + CHUNK_ID_OFFSET);
    }
  }
}
