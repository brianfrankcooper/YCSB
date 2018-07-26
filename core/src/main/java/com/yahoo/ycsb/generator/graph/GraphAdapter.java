package com.yahoo.ycsb.generator.graph;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Serializer for the {@link Graph} class.
 */
public class GraphAdapter implements JsonSerializer<Graph>, JsonDeserializer<Graph> {

  private final String nodes = "nodes";
  private final String edges = "edges";
  private final Gson gson;
  private final Type edgeListType;
  private final Type nodeListType;

  GraphAdapter() {
    gson = new Gson();

    nodeListType = new TypeToken<List<Node>>() {
    }.getType();

    edgeListType = new TypeToken<List<Edge>>() {
    }.getType();
  }

  @Override
  public JsonElement serialize(Graph graph, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject result = new JsonObject();

    JsonElement nodeJsonElement = gson.toJsonTree(graph.getNodes(), nodeListType);
    JsonElement edgeJsonElement = gson.toJsonTree(graph.getEdges(), edgeListType);

    result.add(nodes, nodeJsonElement);
    result.add(edges, edgeJsonElement);

    return result;
  }

  @Override
  public Graph deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws
      JsonParseException {
    Graph graph = new Graph();
    JsonObject jsonObject = jsonElement.getAsJsonObject();

    JsonElement jsonNodes = jsonObject.get(nodes);
    JsonElement jsonEdges = jsonObject.get(edges);

    List<Node> nodeList = gson.fromJson(jsonNodes, nodeListType);
    List<Edge> edgeList = gson.fromJson(jsonEdges, edgeListType);

    nodeList.forEach(graph::addNode);
    edgeList.forEach(graph::addEdge);

    return graph;
  }
}
