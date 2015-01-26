package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Vector;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments.RankerType;
import edu.nyu.cs.cs2580.SearchEngine.Options;

public class MainpageHandler implements HttpHandler {

  private static final String MAINPAGE = "web/index.html";

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();
    if (!"GET".equalsIgnoreCase(requestMethod)) {
      return;
    }
    BufferedReader reader = new BufferedReader(new FileReader(MAINPAGE));
    String line = null;
    StringBuilder sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
      sb.append("\n");
    }
    reader.close();
    String body = sb.toString();
    Headers responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "text/html; charset=UTF-8");
    exchange.sendResponseHeaders(200, 0); 
    OutputStream responseBody = exchange.getResponseBody();
    responseBody.write(body.getBytes("UTF-8"));
    responseBody.close();
  }

}
