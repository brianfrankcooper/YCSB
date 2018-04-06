/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.graph.Edge;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.*;
import org.apache.jena.tdb.TDBFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Apache Jena TDB client for YCSB.
 */
public class ApacheJenaClient extends DB {

  private static final String OUTPUT_DIRECTORY_PROPERTY = "outputdirectory";
  private static final String OUTPUT_DIRECTORY_DEFAULT = new File(System.getProperty("user.dir"),
      "apachejena_database").getAbsolutePath();
  private static final AtomicInteger INIT_COUNT = new AtomicInteger();
  private static final Object INIT_LOCK = new Object();

  private static boolean initialised = false;
  private static Dataset dataset;


  @Override
  public void init() throws DBException {
    super.init();

    INIT_COUNT.incrementAndGet();

    synchronized (INIT_LOCK) {
      if (!initialised) {
        Properties properties = getProperties();
        String outputDirectory = properties.getProperty(OUTPUT_DIRECTORY_PROPERTY, OUTPUT_DIRECTORY_DEFAULT);

        dataset = TDBFactory.createDataset(outputDirectory);

        initialised = true;
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      TDBFactory.release(dataset);
    }

    super.cleanup();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    dataset.begin(ReadWrite.READ);

    try {
      Model model = dataset.getDefaultModel();

      Resource resource = getResource(table, key, model);

      if (fields == null) {
        SimpleSelector simpleSelector;
        if (table.equals(Edge.EDGE_IDENTIFIER)) {
          simpleSelector = new SimpleSelector(null, (Property) resource, (RDFNode) null);
        } else {
          simpleSelector = new SimpleSelector(resource, null, (RDFNode) null);
        }

        StmtIterator stmtIterator = model.listStatements(simpleSelector);

        while (stmtIterator.hasNext()) {
          Statement statement = stmtIterator.nextStatement();
          RDFNode object = statement.getObject();

          addWithLabelOfEdgeToMap(model, result, object, statement.getPredicate(), null);
        }
      } else {
        for (String field : fields) {
          Property property = model.createProperty(field);
          SimpleSelector simpleSelector = new SimpleSelector(resource, property, (RDFNode) null);

          StmtIterator stmtIterator = model.listStatements(simpleSelector);

          while (stmtIterator.hasNext()) {
            Statement statement = stmtIterator.nextStatement();
            RDFNode object = statement.getObject();

            addWithLabelOfEdgeToMap(model, result, object, statement.getPredicate(), fields);
          }
        }
      }

      if (result.isEmpty()) {
        return Status.NOT_FOUND;
      }

    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    dataset.begin(ReadWrite.READ);

    try {
      Model model = dataset.getDefaultModel();

      Resource resource;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Statement statement = model.getProperty(model.createProperty(startkey), model.createProperty(Edge
            .START_IDENTIFIER));

        if (statement == null) {
          return Status.NOT_FOUND;
        }

        resource = model.createResource(new AnonId(statement.getObject().toString()));

        if (isResourceNew(resource)) {
          return Status.NOT_FOUND;
        }
      } else {
        resource = getResource(table, startkey, model);
      }

      // Scans the data set in a depth first fashion.
      scanFields(model, resource, recordcount, fields, result);
    } finally {
      dataset.end();
    }

    if (result.size() == 0) {
      return Status.NOT_FOUND;
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    dataset.begin(ReadWrite.WRITE);

    try {
      Model model = dataset.getDefaultModel();
      Resource resource = getResource(table, key, model);

      List<Statement> statements = new ArrayList<>();

      for (String field : values.keySet()) {
        Property property = model.createProperty(field);
        SimpleSelector simpleSelector = new SimpleSelector(resource, property, (RDFNode) null);

        StmtIterator stmtIterator = model.listStatements(simpleSelector);

        stmtIterator.forEachRemaining(statement ->
            statements.add(model.createStatement(resource, statement.getPredicate(), values.get(field).toString())));
      }

      model.add(statements);

      dataset.commit();
    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    dataset.begin(ReadWrite.WRITE);

    try {
      Model model = dataset.getDefaultModel();

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Resource startNode = model.createResource(new AnonId(values.get(Edge.START_IDENTIFIER).toString()));

        Property property = model.createProperty(key);
        addEdgeProperties(model, property, values);

        Resource endNode = model.createResource(new AnonId(values.get(Edge.END_IDENTIFIER).toString()));

        model.add(startNode, property, endNode);
      } else {
        Resource resource = model.createResource(new AnonId(key));
        List<Statement> statements = createStatementsForNode(model, resource, values);

        if (statements.isEmpty()) {
          return Status.ERROR;
        }

        model.add(statements);
      }

      dataset.commit();
    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    dataset.begin(ReadWrite.WRITE);

    try {
      Model model = dataset.getDefaultModel();

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Property property = model.getProperty(key);

        model.removeAll(null, property, null);
      } else {
        Resource resource = getResource(table, key, model);

        model.removeAll(resource, null, null);
        model.removeAll(null, null, resource);
      }

      dataset.commit();
    } finally {
      dataset.end();
    }

    return Status.OK;
  }

  private boolean isResourceNew(Resource resource) {
    return !resource.listProperties().hasNext();
  }

  private void scanFields(Model model,
                          Resource resource,
                          int recordcount,
                          Set<String> fields,
                          Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() == recordcount) {
      return;
    }

    HashMap<String, ByteIterator> map = new HashMap<>();
    result.add(map);

    StmtIterator stmtIterator = resource.listProperties();

    while (stmtIterator.hasNext()) {
      Statement statement = stmtIterator.nextStatement();
      RDFNode object = statement.getObject();

      addWithLabelOfEdgeToMap(model, map, object, statement.getPredicate(), fields);

      if (object.isResource()) {
        scanFields(model, object.asResource(), recordcount, fields, result);
      }
    }

    if (map.isEmpty()) {
      result.remove(map);
    }
  }

  private void addWithLabelOfEdgeToMap(Model model,
                                       Map<String, ByteIterator> map,
                                       RDFNode object,
                                       Property property,
                                       Set<String> fields) {
    Statement statement = model.getProperty(property, model.createProperty(Edge.LABEL_IDENTIFIER));

    if (statement == null && (fields == null || fields.contains(property.toString()))) {
      map.put(property.toString(), new StringByteIterator(object.toString()));
    } else if (statement != null && (fields == null || fields.contains(property.toString()))) {
      map.put(statement.getObject().toString(), new StringByteIterator(object.toString()));
    }
  }

  private List<Statement> createStatementsForNode(Model model, Resource resource, Map<String, ByteIterator> values) {
    List<Statement> statements = new ArrayList<>();

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      Statement statement = model.createStatement(
          resource,
          model.createProperty(entry.getKey()),
          entry.getValue().toString());

      statements.add(statement);
    }

    return statements;
  }

  private void addEdgeProperties(Model model, Property property, Map<String, ByteIterator> values) {
    for (String field : Edge.EDGE_FIELDS_SET) {
      if (field.equals(Edge.START_IDENTIFIER) || field.equals(Edge.END_IDENTIFIER)) {
        property.addProperty(model.createProperty(field), model.createResource(values.get(field).toString()));
      } else {
        property.addProperty(model.createProperty(field), values.get(field).toString());
      }
    }
  }

  private Resource getResource(String table, String key, Model model) {
    Resource resource;
    if (table.equals(Edge.EDGE_IDENTIFIER)) {
      resource = model.createProperty(key);
    } else {
      resource = model.createResource(new AnonId(key));
    }
    return resource;
  }
}