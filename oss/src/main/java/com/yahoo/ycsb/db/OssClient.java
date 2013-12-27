/**
 * Oss client binding for YCSB.
 * 
 * This is Aliyun Oss Server DB client.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.aliyun.openservices.oss.*;
import com.aliyun.openservices.oss.model.*;
import com.aliyun.openservices.oss.model.ListObjectsRequest;


public class OssClient extends DB {
   
    private OSSClient mOssClient;


    public void init() throws DBException {
        Properties props = getProperties();
        String host = props.getProperty("oss.host", "http://oss.aliyuncs.com");
        String accessid = props.getProperty("oss.accessid", "HrLKh0FZmx9eD9RM");
        String accesskey = props.getProperty("oss.accesskey", "PqhHjx6XCbxYZhvNJjVwHRcYt3kb6x");

        System.out.println(host);
        System.out.println(accessid);
        System.out.println(accesskey);

        this.mOssClient = new OSSClient(host, accessid, accesskey) ; 
    }

    public void cleanup() throws DBException {
        System.out.println("Clean up....");
    }

    /* Calculate a hash for a key to store it in an index.  The actual return
     * value of this function is not interesting -- it primarily needs to be
     * fast and scattered along the whole space of doubles.  In a real world
     * scenario one would probably use the ASCII values of the keys.
     */
    private double hash(String key) {
        return key.hashCode();
    }

    /*
     * Get object content
     */
    public String getObject(String bucketName, String key){
        OSSObject object = this.mOssClient.getObject(bucketName, key);
        StringBuilder out = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        try{
        for(String line = br.readLine(); line != null; line = br.readLine())
            out.append(line);
        br.close();
        }catch (IOException e){
            System.err.println("Caught IOException:" + e.getMessage()) ;
        }
        return out.toString();
    }
    
    /*
     * Put object content
     */
    public void putObject(String bucketName, String objectName, String value){
        try
        {
            if (!this.mOssClient.doesBucketExist(bucketName))
                this.mOssClient.createBucket(bucketName);
        }
        catch (Exception e){
            System.err.println("Caught Exception:" + e.getMessage());
        }

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(value.length()) ;
        try
        {
            InputStream in = new ByteArrayInputStream(value.getBytes("UTF-8")) ;
            PutObjectResult result = this.mOssClient.putObject(bucketName, objectName, in, meta) ;
            System.out.println(result.getETag()) ;
        }
        catch(UnsupportedEncodingException e){
            System.err.println("Caught IOException:" + e.getMessage()) ;
        }
    }

    /*
     * Update object content
     */
    public void updateObject(String bucketName, String objectName, String value) {
        this.putObject(bucketName, objectName, value) ;
    }

    /*
     * Get objects with prefix 
     */
    public List<String> getPrefixObject(String bucketName, String prefix)
    {
        List<String> objects = new ArrayList<String>() ;
        ListObjectsRequest listobjectsrequest = new ListObjectsRequest(bucketName) ; 
        listobjectsrequest.setDelimiter("/");
        listobjectsrequest.setPrefix(prefix);
        ObjectListing listing = this.mOssClient.listObjects(listobjectsrequest);
        for( OSSObjectSummary objectSummary : listing.getObjectSummaries())
        {
            objects.add( objectSummary.getKey()) ;
        }
        return objects;
    }


    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
            //table = "ycsb";
            if( fields != null){
            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
            List<String> values = new ArrayList<String>();
            for(int i = 0;i < fieldArray.length;i ++){
                StringBuilder objectName = new StringBuilder();
                objectName.append(key) ;
                objectName.append("/") ;
                objectName.append(fieldArray[i]) ;
                values.add( getObject(table,objectName.toString()) ) ;
            }

            Iterator<String> fieldIterator = fields.iterator();
            Iterator<String> valueIterator = values.iterator();

            while (fieldIterator.hasNext() && valueIterator.hasNext()) {
                result.put(fieldIterator.next(),
			   new StringByteIterator(valueIterator.next()));
            }
            assert !fieldIterator.hasNext() && !valueIterator.hasNext();
            return result.isEmpty() ? 1 : 0;
            }
         else {
             System.out.println(" fields is null ") ;
             return 1;
         }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values){
        //table = "ycsb";
        if(values.size() == 0) return 1;
        for( String k: values.keySet()){
                StringBuilder objectName = new StringBuilder();
                objectName.append(key) ;
                objectName.append("/") ;
                objectName.append(k) ;
                System.out.println(objectName);
                this.putObject(table, objectName.toString(), values.get(k).toString()) ;
        }
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        //table = "ycsb";
        List<String> objects = this.getPrefixObject(table, key) ; 
        for(String object:objects){
            this.mOssClient.deleteObject(table, object) ;
        }
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
      //table = "ycsb";
      return this.insert(table, key, values) ;  
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("Scan Command");
        return 0;
    }

}
