package site.ycsb.workloads;

import site.ycsb.DB;
import site.ycsb.WorkloadException;
import site.ycsb.Status;

import java.util.Properties;

public class TransactionWorkload extends CoreWorkload {

  public static final String TRANSACTION_READ_COUNT_PROPERTY = "transactionReadCount";
  public static final String TRANSACTION_READ_COUNT_DEFAULT = "5";

  public static final String TRANSACTION_UPDATE_COUNT_PROPERTY = "transactionUpdateCount";
  public static final String TRANSACTION_UPDATE_COUNT_DEFAULT = "5";

  public static final String TRANSACTION_INSERT_COUNT_PROPERTY = "transactionInsertCount";
  public static final String TRANSACTION_INSERT_COUNT_DEFAULT = "5";

  public static final String TRANSACTION_SCAN_COUNT_PROPERTY = "transactionScanCount";
  public static final String TRANSACTION_SCAN_COUNT_DEFAULT = "5";



  private int transactionReadCount;
  private int transactionUpdateCount;
  private int transactionInsertCount;
  private int transactionScanCount;




  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);

    transactionReadCount = Integer.parseInt(p.getProperty(TRANSACTION_READ_COUNT_PROPERTY, TRANSACTION_READ_COUNT_DEFAULT));
    transactionUpdateCount = Integer.parseInt(p.getProperty(TRANSACTION_UPDATE_COUNT_PROPERTY, TRANSACTION_UPDATE_COUNT_DEFAULT));
    transactionInsertCount = Integer.parseInt(p.getProperty(TRANSACTION_INSERT_COUNT_PROPERTY, TRANSACTION_INSERT_COUNT_DEFAULT));
    transactionScanCount = Integer.parseInt(p.getProperty(TRANSACTION_SCAN_COUNT_PROPERTY, TRANSACTION_SCAN_COUNT_DEFAULT));

  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {

    Status status = db.start();

    for(int i = 0; i < transactionReadCount && status.isOk(); i++) {
      status = doTransactionRead(db);
    }

    for(int i = 0; i < transactionUpdateCount && status.isOk(); i++) {
      status = doTransactionUpdate(db);
    }

    for(int i = 0; i < transactionInsertCount && status.isOk(); i++) {
      status = doTransactionInsert(db);
    }

    for (int i = 0; i < transactionScanCount && status.isOk(); i++) {
      status = doTransactionScan(db);
    }

    if(!status.isOk()) {
      db.rollback();
    } else {
      db.commit();
    }

    return true;
  }
}
