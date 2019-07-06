/**
 * 
 * VoltDBTableSortedMergeWrangler allows you to merge an array of VoltTable
 * provided by callAllPartitionProcedure.
 * 
 * The intended use case is for when you need to issue a multi partition query
 * but would prefer not to, as you don't need perfect read consistency and would
 * rather get the individual VoltDB partitions to issue the query independently
 * and then somehow merge the results.
 *
 */
package org.voltdb.sortedvolttable;
