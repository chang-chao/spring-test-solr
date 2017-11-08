package jp.co.ntt_at.cis.horei_db.tools.solr;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.skyscreamer.jsonassert.JSONParser;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.util.FileCopyUtils;

public class SolrTestExecutionListener extends AbstractTestExecutionListener {
  private SolrClient solrClient;

  private String collection;

  public SolrTestExecutionListener() throws IOException {
    super();
    final Properties properties = new Properties();
    try (final InputStream stream = this.getClass().getResourceAsStream("/solr-test.properties")) {
      properties.load(stream);
    }

    this.solrClient = new HttpSolrClient.Builder(properties.getProperty("baseUrl")).build();
    this.collection = properties.getProperty("collection");
  }



  @SuppressWarnings("unchecked")
  @Override
  public void beforeTestMethod(TestContext testContext) throws Exception {
    doInTransaction(() -> {
      solrClient.deleteByQuery(collection, "*:*");
      SolrSetup setupAnnotation =
          AnnotationUtils.getAnnotation(testContext.getTestMethod(), SolrSetup.class);
      if (setupAnnotation == null) {
        return;
      }
      String jsonFile = setupAnnotation.value();

      String inputJson = FileCopyUtils.copyToString(new InputStreamReader(
          testContext.getTestClass().getResourceAsStream(jsonFile), StandardCharsets.UTF_8));
      JSONArray inputData = (JSONArray) JSONParser.parseJSON(inputJson);
      for (int i = 0; i < inputData.length(); i++) {
        JSONObject object = (JSONObject) inputData.get(i);
        SolrInputDocument solrDoc = new SolrInputDocument();
        object.keys().forEachRemaining(key -> {
          try {
            solrDoc.addField((String) key, object.get((String) key));
          } catch (JSONException e) {
            throw new RuntimeException(e);
          }
        });
        solrClient.add(collection, solrDoc);
      }
    });
  }

  @Override
  public void afterTestMethod(TestContext testContext) throws Exception {

    ExpectedSolr expectedAnnotation =
        AnnotationUtils.getAnnotation(testContext.getTestMethod(), ExpectedSolr.class);
    if (expectedAnnotation == null) {
      return;
    }

    String jsonFile = expectedAnnotation.value();

    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = solrClient.query(collection, query);
    SolrDocumentList list = response.getResults();
    JSONArray docArray = new JSONArray();
    for (SolrDocument solrDocument : list) {
      docArray.put(new JSONObject(solrDocument));
    }
    String responseInJson = docArray.toString();

    String expectedStr = FileCopyUtils.copyToString(new InputStreamReader(
        testContext.getTestClass().getResourceAsStream(jsonFile), StandardCharsets.UTF_8));
    JSONCompareResult result =
        JSONCompare.compareJSON(expectedStr, responseInJson, JSONCompareMode.LENIENT);
    Assert.assertFalse(result.getMessage(), result.failed());

  }


  /**
   * Solrへの本文の投入.
   *
   * @throws Exception
   */
  private void doInTransaction(SolrOperation r) throws Exception {
    r.execute();
    solrClient.commit(collection);
  }

  /**
   * Solrの実行.
   */
  @FunctionalInterface
  private static interface SolrOperation {
    void execute() throws Exception;
  }

}
