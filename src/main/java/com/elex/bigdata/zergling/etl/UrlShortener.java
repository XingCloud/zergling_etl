package com.elex.bigdata.zergling.etl;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu Date: 14-3-27 Time: 下午5:17 Package: com.elex.bigdata.zergling.etl
 */
public class UrlShortener {
  private static class InstanceHolder {
    private static final UrlShortener instance = new UrlShortener();
  }

  public synchronized static UrlShortener getInstance() {
    return InstanceHolder.instance;
  }

  private UrlShortener() {
    localCache = new HashMap<>(500);
  }

  private final HttpClient CLIENT = HttpClients.createDefault();
  private final String HTTP_SCHEMA = "http";
  private final String HOST = "restore.url";
  private final int PORT = 8080;
  private final String PATH = "/ur/r";

  private Map<String, String> localCache;

  // Concurrent is not important here, so ignore synchronization.
  public String toOriginalURL(String shortURL) throws Exception {
    String originalURL = localCache.get(shortURL);
    if (StringUtils.isNotBlank(originalURL)) {
      return originalURL;
    }
    URIBuilder builder = new URIBuilder();
    builder.setScheme(HTTP_SCHEMA);
    builder.setHost(HOST);
    builder.setPort(PORT);
    builder.setPath(PATH);
    builder.addParameter("url", shortURL);

    URI uri = builder.build();
    HttpGet httpGet = new HttpGet(uri);
    HttpResponse response = CLIENT.execute(httpGet);
    StatusLine statusLine = response.getStatusLine();
    int c = statusLine.getStatusCode();
    String result;
    if (200 == c) {
      result = StringUtils.trimToNull(readResponse(response.getEntity()));
      if (StringUtils.isBlank(result) || "err".equals(result)) {
        throw new Exception("Invalid url result - " + shortURL);
      } else {
        originalURL = result;
      }
    } else {
      throw new Exception("Error return code: " + String.valueOf(c));
    }
    localCache.put(shortURL, originalURL);
    return originalURL;
  }

  private String readResponse(HttpEntity responseEntity) throws IOException {
    if (responseEntity == null) {
      return null;
    }
    StringBuilder responseString = new StringBuilder();
    String line;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseEntity.getContent()));) {
      while ((line = reader.readLine()) != null) {
        line = StringUtils.trimToNull(line);
        if (StringUtils.isNotEmpty(line)) {
          responseString.append(line);
        }
      }
    }
    return responseString.toString();
  }
}
