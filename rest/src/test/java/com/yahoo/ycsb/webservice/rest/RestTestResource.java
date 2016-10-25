/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.webservice.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Class that implements a mock RESTFul web service to be used for integration
 * testing.
 */
@Path("/resource/{id}")
public class RestTestResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response respondToGET(@PathParam("id") String id) {
    return processRequests(id, HttpMethod.GET);
  }
  
  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public Response respondToPOST(@PathParam("id") String id) {
    return processRequests(id, HttpMethod.POST);
  }

  @DELETE
  @Produces(MediaType.TEXT_PLAIN)
  public Response respondToDELETE(@PathParam("id") String id) {
    return processRequests(id, HttpMethod.DELETE);
  }

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  public Response respondToPUT(@PathParam("id") String id) {
    return processRequests(id, HttpMethod.PUT);
  }
  
  private static Response processRequests(String id, String method) {
    if (id.equals("resource_invalid"))
      return Response.serverError().build();
    else if (id.equals("resource_absent"))
      return Response.status(Response.Status.NOT_FOUND).build();
    else if (id.equals("resource_unauthorized"))
      return Response.status(Response.Status.FORBIDDEN).build();
    return Response.ok("HTTP " + method + " response to: " + id).build();
  }
}