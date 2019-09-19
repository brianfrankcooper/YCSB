package site.ycsb.db.voltdb.sortedvolttable;

/**
 * Copyright (c) 2015-2019 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */ 

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;

/**
 * VoltDBTableSortedMergeWrangler allows you to merge an array of VoltTable
 * provided by callAllPartitionProcedure.
 * 
 * The intended use case is for when you need to issue a multi partition query
 * but would prefer not to, as you don't need perfect read consistency and would
 * rather get the individual VoltDB partitions to issue the query independently
 * and then somehow merge the results.
 * 
 */
public class VoltDBTableSortedMergeWrangler {

  private ClientResponseWithPartitionKey[] theTables = null;
  @SuppressWarnings("rawtypes")
  private Comparable whatWeSelectedLastTime = null;

  public VoltDBTableSortedMergeWrangler(ClientResponseWithPartitionKey[] response) {
    super();
    this.theTables = response;
  }

  /**
   * Takes 'theTables' and merges them based on column 'columnId'. We assume that
   * column 'columnId' in each element of 'theTables' is correctly sorted within
   * itself.
   * 
   * @param columnid
   * @param limit    How many rows we want
   * @return A new VoltTable.
   * @throws NeedsToBeComparableException - if column columnId doesn't implement Comparable.
   * @throws IncomingVoltTablesNeedToBeSortedException - incoming data isn't already sorted.
   * @throws ClientResponseIsBadException - The procedure worked but is complaining.
   */
  public VoltTable getSortedTable(int columnid, int limit)
      throws NeedsToBeComparableException, IncomingVoltTablesNeedToBeSortedException, ClientResponseIsBadException {

    whatWeSelectedLastTime = null;

    // Create an empty output table
    VoltTable outputTable = new VoltTable(theTables[0].response.getResults()[0].getTableSchema());

    // make sure our input tables are usable, and ready to be read from the
    // start
    for (int i = 0; i < theTables.length; i++) {
      VoltTable currentTable = theTables[i].response.getResults()[0];

      if (theTables[i].response.getStatus() != ClientResponse.SUCCESS) {
        throw new ClientResponseIsBadException(i + " " + theTables[i].response.getStatusString());
      }

      currentTable.resetRowPosition();
      currentTable.advanceRow();
    }

    // Find table with lowest value for columnId, which is supposed to be
    // the sort key.
    int lowestId = getLowestId(columnid);

    // Loop until we run out of data or get 'limit' rows.
    while (lowestId > -1 && outputTable.getRowCount() < limit) {

      // having identified the lowest Table pull that row, add it to
      // the output table, and then call 'advanceRow' so we can do this
      // again...
      VoltTable lowestTable = theTables[lowestId].response.getResults()[0];
      outputTable.add(lowestTable.cloneRow());
      lowestTable.advanceRow();

      // Find table with lowest value for columnId
      lowestId = getLowestId(columnid);
    }

    return outputTable;
  }

  /**
   * This routine looks at column 'columnId' in an array of VoltTable and
   * identifies which one is lowest. Note that as we call 'advanceRow' elsewhere
   * this will change.
   * 
   * @param columnid
   * @return the VoltTable with the lowest value for column 'columnId'. or -1 if
   *         we've exhausted all the VoltTables.
   * @throws NeedsToBeComparableException
   * @throws IncomingVoltTablesNeedToBeSortedException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private int getLowestId(int columnid) throws NeedsToBeComparableException, IncomingVoltTablesNeedToBeSortedException {

    int lowestId = -1;
    Comparable lowestObservedValue = null;

    for (int i = 0; i < theTables.length; i++) {

      VoltTable currentTable = theTables[i].response.getResults()[0];

      int activeRowIndex = currentTable.getActiveRowIndex();
      int rowCount = currentTable.getRowCount();

      if (activeRowIndex > -1 && activeRowIndex < rowCount) {

        if (lowestObservedValue == null) {

          lowestId = i;
          lowestObservedValue = getComparable(currentTable, columnid);

        } else {
          Comparable newObservedValue = getComparable(currentTable, columnid);

          if (newObservedValue.compareTo(lowestObservedValue) <= 0) {
            lowestId = i;

            lowestObservedValue = getComparable(currentTable, columnid);
          }
        }

      }
    }

    // If we found something make sure that the data in columnid was sorted
    // properly when it was retrieved.
    if (lowestId > -1) {
      Comparable latestItemWeSelected = getComparable(theTables[lowestId].response.getResults()[0], columnid);

      if (whatWeSelectedLastTime != null && latestItemWeSelected.compareTo(whatWeSelectedLastTime) < 0) {
        throw new IncomingVoltTablesNeedToBeSortedException(
            "Latest Item '" + latestItemWeSelected + "' is before last item '" + whatWeSelectedLastTime + "'");
      }

      whatWeSelectedLastTime = latestItemWeSelected;
    }

    return lowestId;

  }

  /**
   * Get the value we're working with as a Comparable.
   * 
   * @param theTable
   * @param columnId
   * @return a Comparable.
   * @throws NeedsToBeComparableException
   */
  @SuppressWarnings("rawtypes")
  private Comparable getComparable(VoltTable theTable, int columnId) throws NeedsToBeComparableException {
    Comparable c = null;

    VoltType vt = theTable.getColumnType(columnId);
    Object theValue = theTable.get(columnId, vt);

    if (theValue instanceof Comparable) {
      c = (Comparable) theValue;
    } else {
      throw new NeedsToBeComparableException(
          theValue + ": Only Comparables are supported by VoltDBTableSortedMergeWrangler");
    }

    return c;
  }

  /**
   * Do a comparison of byte arrays. Not used right now, but will be when we added
   * support for VARBINARY.
   * 
   * @param left
   * @param right
   * @return whether 'left' is <, >, or = 'right'
   */
  private int compare(byte[] left, byte[] right) {
    for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {

      int a = (left[i] & 0xff);
      int b = (right[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }
}
