/*
 * Copyright © 2016-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.admin;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Tests whether admin operations work in program contexts.
 */
public class AdminAppTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final ArtifactId ADMIN_APP_ARTIFACT = NamespaceId.DEFAULT.artifact("admin-app", "1.0.0");
  private static final ArtifactSummary ADMIN_ARTIFACT_SUMMARY = new ArtifactSummary(ADMIN_APP_ARTIFACT.getArtifact(),
                                                                                    ADMIN_APP_ARTIFACT.getVersion());

  private ApplicationManager appManager;

  @Before
  public void deploy() throws Exception {
    addAppArtifact(ADMIN_APP_ARTIFACT, AdminApp.class);
    AppRequest<Void> appRequest = new AppRequest<>(ADMIN_ARTIFACT_SUMMARY);
    appManager = deployApplication(NamespaceId.DEFAULT.app("AdminApp"), appRequest);
  }

  @Test
  public void testAdminFlow() throws Exception {

    // start the worker and wait for it to finish
    FlowManager flowManager = appManager.getFlowManager(AdminApp.FLOW_NAME).start();

    try {
      flowManager.waitForRun(ProgramRunStatus.RUNNING, 5, TimeUnit.MINUTES);

      // send some events to the stream
      StreamManager streamManager = getStreamManager("events");
      streamManager.send("aa ab bc aa bc");
      streamManager.send("xx xy aa ab aa");

      // wait for flow to process them
      flowManager.getFlowletMetrics("counter").waitForProcessed(10, 30, TimeUnit.SECONDS);

      // validate that the flow created tables for a, b, and x, and that the counts are correct
      DataSetManager<KeyValueTable> aManager = getDataset("counters_a");
      Assert.assertNotNull(aManager.get());
      Assert.assertEquals(4L, Bytes.toLong(aManager.get().read("aa")));
      Assert.assertEquals(2L, Bytes.toLong(aManager.get().read("ab")));

      DataSetManager<KeyValueTable> bManager = getDataset("counters_b");
      Assert.assertNotNull(bManager.get());
      Assert.assertEquals(2L, Bytes.toLong(bManager.get().read("bc")));

      DataSetManager<KeyValueTable> xManager = getDataset("counters_x");
      Assert.assertNotNull(xManager.get());
      Assert.assertEquals(1L, Bytes.toLong(xManager.get().read("xx")));
      Assert.assertEquals(1L, Bytes.toLong(xManager.get().read("xy")));

    } finally {
      flowManager.stop();
    }
    flowManager.waitForRun(ProgramRunStatus.KILLED, 30, TimeUnit.SECONDS);

    // flowlet destroy() deletes all the tables - validate
    Assert.assertNull(getDataset("counters_a").get());
    Assert.assertNull(getDataset("counters_b").get());
    Assert.assertNull(getDataset("counters_x").get());
  }

  @Test
  public void testAdminWorker() throws Exception {
    testAdminProgram(appManager.getWorkerManager(AdminApp.WORKER_NAME));
  }

  @Test
  public void testAdminWorkflow() throws Exception {
    testAdminProgram(appManager.getWorkflowManager(AdminApp.WORKFLOW_NAME));
  }

  private <T extends ProgramManager<T>>
  void testAdminProgram(ProgramManager<T> manager) throws Exception {

    // create fileset b; it will be updated by the worker
    addDatasetInstance(FileSet.class.getName(), "b", FileSetProperties
      .builder().setBasePath("some/path").setInputFormat(TextInputFormat.class).build());
    DataSetManager<FileSet> bManager = getDataset("b");
    String bFormat = bManager.get().getInputFormatClassName();
    String bPath = bManager.get().getBaseLocation().toURI().getPath();
    Assert.assertTrue(bPath.endsWith("some/path/"));
    bManager.flush();

    // create table c and write some data to it; it will be truncated by the worker
    addDatasetInstance("table", "c");
    DataSetManager<Table> cManager = getDataset("c");
    cManager.get().put(new Put("x", "y", "z"));
    cManager.flush();

    // create table d; it will be dropped by the worker
    addDatasetInstance("table", "d");

    // start the worker and wait for it to finish
    File newBasePath = new File(TMP_FOLDER.newFolder(), "extra");
    Assert.assertFalse(newBasePath.exists());
    manager.start(ImmutableMap.of("new.base.path", newBasePath.getPath()));
    manager.waitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    // validate that worker created dataset a
    DataSetManager<Table> aManager = getDataset("a");
    Assert.assertNull(aManager.get().scan(null, null).next());
    aManager.flush();

    // validate that worker update fileset b, Get a new instance of b
    bManager = getDataset("b");
    Assert.assertEquals(bFormat, bManager.get().getInputFormatClassName());
    String newBPath = bManager.get().getBaseLocation().toURI().getPath();
    Assert.assertTrue(newBPath.endsWith("/extra/"));
    // make sure the directory was created by fileset update (by moving the existing base path)
    Assert.assertTrue(newBasePath.exists());
    bManager.flush();

    // validate that dataset c is empty
    Assert.assertNull(cManager.get().scan(null, null).next());
    cManager.flush();

    // validate that dataset d is gone
    Assert.assertNull(getDataset("d").get());

    // run the worker again to drop all datasets
    manager.start(ImmutableMap.of("dropAll", "true"));
    manager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 30, TimeUnit.SECONDS);

    Assert.assertNull(getDataset("a").get());
    Assert.assertNull(getDataset("b").get());
    Assert.assertNull(getDataset("c").get());
    Assert.assertNull(getDataset("d").get());
  }

  @Test
  public void testAdminService() throws Exception {

    // Start the service
    ServiceManager serviceManager = appManager.getServiceManager(AdminApp.SERVICE_NAME).start();
    String namespaceX = "x";
    ArtifactId pluginArtifactId = new NamespaceId(namespaceX).artifact("r1", "1.0.0");

    try {
      getNamespaceAdmin().create(new NamespaceMeta.Builder().setName(namespaceX).build());

      URI serviceURI = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI();

      // dataset nn should not exist
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("exists/nn").toURL()), namespaceX, 200, "false");

      // create nn as a table in both namespaces
      executeInNamespaces(() -> HttpRequest.put(serviceURI.resolve("create/nn/table").toURL()), namespaceX, 200);

      // now nn should exist
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("exists/nn").toURL()), namespaceX, 200, "true");

      // create it again as a fileset -> should fail with conflict
      executeInNamespaces(() -> HttpRequest.put(serviceURI.resolve("create/nn/fileSet").toURL()), namespaceX, 409);

      // get the type for xx -> not found
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("type/xx").toURL()), namespaceX, 404);

      // get the type for nn -> table
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("type/nn").toURL()), namespaceX, 200, "table");

      // update xx's properties -> should get not-found
      Map<String, String> nnProps = TableProperties.builder().setTTL(1000L).build().getProperties();
      executeInNamespaces(() -> HttpRequest.put(serviceURI.resolve("update/xx").toURL()).withBody(GSON.toJson(nnProps)),
                          namespaceX, 404);

      // update nn's properties
      executeInNamespaces(() -> HttpRequest.put(serviceURI.resolve("update/nn").toURL()).withBody(GSON.toJson(nnProps)),
                          namespaceX, 200);

      // get properties for xx -> not found
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("props/xx").toURL()), namespaceX, 404);

      // get properties for nn and validate
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("props/nn").toURL()), namespaceX,
                          response -> {
                            Assert.assertEquals(200, response.getResponseCode());

                            Map<String, String> returnedProps = GSON.fromJson(response.getResponseBodyAsString(),
                                                                              MAP_TYPE);
                            Assert.assertEquals(nnProps, returnedProps);
                          });

      // write some data to the table
      DataSetManager<Table> nnManager = getDataset("nn");
      nnManager.get().put(new Put("x", "y", "z"));
      nnManager.flush();

      // in a new tx, validate that data is in table
      Assert.assertFalse(nnManager.get().get(new Get("x")).isEmpty());
      Assert.assertEquals("z", nnManager.get().get(new Get("x", "y")).getString("y"));
      nnManager.flush();

      // truncate xx -> not found
      executeInNamespaces(() -> HttpRequest.post(serviceURI.resolve("truncate/xx").toURL()), namespaceX, 404);

      // truncate nn
      executeInNamespaces(() -> HttpRequest.post(serviceURI.resolve("truncate/nn").toURL()), namespaceX, 200);

      // validate table is empty
      Assert.assertTrue(nnManager.get().get(new Get("x")).isEmpty());
      nnManager.flush();

      // delete nn
      executeInNamespaces(() -> HttpRequest.delete(serviceURI.resolve("delete/nn").toURL()), namespaceX, 200);

      // delete again -> not found
      executeInNamespaces(() -> HttpRequest.delete(serviceURI.resolve("delete/nn").toURL()), namespaceX, 404);

      // delete xx which never existed -> not found
      executeInNamespaces(() -> HttpRequest.delete(serviceURI.resolve("delete/xx").toURL()), namespaceX, 404);

      // exists should now return false for nn
      executeInNamespaces(() -> HttpRequest.get(serviceURI.resolve("exists/nn").toURL()), namespaceX, 200, "false");

      Assert.assertNull(getDataset("nn").get());
      Assert.assertNull(getDataset(new NamespaceId(namespaceX).dataset("nn")).get());

      // test Admin.namespaceExists()
      HttpRequest request = HttpRequest.get(serviceURI.resolve("plugins").toURL())
        .addHeader(AdminApp.NAMESPACE_HEADER, "y").build();
      HttpResponse response = HttpRequests.execute(request);
      Assert.assertEquals(404, response.getResponseCode());

      // test ArtifactManager.listArtifacts()
      // add a plugin artifact to namespace X
      addPluginArtifact(pluginArtifactId, ADMIN_APP_ARTIFACT, DummyPlugin.class);
      // no plugins should be listed in the default namespace, but the app artifact should
      request = HttpRequest.get(serviceURI.resolve("plugins").toURL()).build();
      response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
      Type setType = new TypeToken<Set<ArtifactSummary>>() { }.getType();
      Assert.assertEquals(Collections.singleton(ADMIN_ARTIFACT_SUMMARY),
                          GSON.fromJson(response.getResponseBodyAsString(), setType));
      // the plugin should be listed in namespace X
      request = HttpRequest.get(serviceURI.resolve("plugins").toURL())
        .addHeader(AdminApp.NAMESPACE_HEADER, namespaceX).build();
      response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
      ArtifactSummary expected = new ArtifactSummary(pluginArtifactId.getArtifact(), pluginArtifactId.getVersion());
      Assert.assertEquals(Collections.singleton(expected), GSON.fromJson(response.getResponseBodyAsString(), setType));

    } finally {
      serviceManager.stop();

      getNamespaceAdmin().delete(new NamespaceId(namespaceX));
    }
  }

  @Test
  public void testAdminSpark() throws Exception {
    testAdminBatchProgram(appManager.getSparkManager(AdminApp.SPARK_NAME));
  }

  @Test
  public void testAdminScalaSpark() throws Exception {
    testAdminBatchProgram(appManager.getSparkManager(AdminApp.SPARK_SCALA_NAME));
  }

  @Test
  public void testAdminMapReduce() throws Exception {
    testAdminBatchProgram(appManager.getMapReduceManager(AdminApp.MAPREDUCE_NAME));
  }

  private <T extends ProgramManager<T>>
  void testAdminBatchProgram(ProgramManager<T> manager) throws Exception {

    addDatasetInstance("keyValueTable", "lines");
    addDatasetInstance("keyValueTable", "counts");

    // add some lines to the input dataset
    DataSetManager<KeyValueTable> linesManager = getDataset("lines");
    linesManager.get().write("1", "hello world");
    linesManager.get().write("2", "hi world");
    linesManager.flush();

    // add some counts to the output dataset
    DataSetManager<KeyValueTable> countsManager = getDataset("counts");
    countsManager.get().write("you", Bytes.toBytes(5));
    countsManager.get().write("me", Bytes.toBytes(3));
    countsManager.flush();

    manager = manager.start();
    manager.waitForRun(ProgramRunStatus.COMPLETED, 180, TimeUnit.SECONDS);

    // validate that there are no counts for "you" and "me", and the the other counts are accurate
    countsManager.flush(); // need to start a new tx to see the output of MR
    Assert.assertEquals(2, Bytes.toInt(countsManager.get().read("world")));
    Assert.assertEquals(1, Bytes.toInt(countsManager.get().read("hello")));
    Assert.assertEquals(1, Bytes.toInt(countsManager.get().read("hi")));
    Assert.assertNull(countsManager.get().read("you"));
    Assert.assertNull(countsManager.get().read("me"));
    countsManager.flush();
  }

  private void executeInNamespaces(RequestSupplier requestSupplier, String namespace,
                                   int expectedCode) throws Exception {
    executeInNamespaces(requestSupplier, namespace, expectedCode, null);
  }

  private void executeInNamespaces(RequestSupplier requestSupplier, String namespace,
                                   int expectedCode, @Nullable String expectedResponse) throws Exception {
    executeInNamespaces(requestSupplier, namespace, response -> {
      Assert.assertEquals(expectedCode, response.getResponseCode());
      if (expectedResponse != null) {
        Assert.assertEquals(expectedResponse, response.getResponseBodyAsString());
      }
    });
  }

  // execute an HTTP request once without setting a namespace header and once with setting it
  private void executeInNamespaces(RequestSupplier requestSupplier, String namespace,
                                   Consumer<HttpResponse> responseConsumer) throws Exception {
    HttpRequest request = requestSupplier.get().build();
    HttpResponse response = HttpRequests.execute(request);
    responseConsumer.accept(response);

    request = requestSupplier.get().addHeader(AdminApp.NAMESPACE_HEADER, namespace).build();
    response = HttpRequests.execute(request);
    responseConsumer.accept(response);
  }

  /**
   * Used instead of a Supplier so that throwing exceptions is allowed
   */
  private interface RequestSupplier {
    HttpRequest.Builder get() throws Exception;
  }
}
