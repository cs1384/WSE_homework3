package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Vector;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import edu.nyu.cs.cs2580.PseudoRev.WPPair;
import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.util.List;

/**
 * Handles each incoming query, students do not need to change this class except
 * to provide more query time CGI arguments and the HTML output.
 *
 * N.B. This class is not thread-safe.
 *
 * @author congyu
 * @author fdiaz
 */
class PRFHandler implements HttpHandler
{

    // For accessing the underlying documents to be used by the Ranker. Since 
    // we are not worried about thread-safety here, the Indexer class must take
    // care of thread-safety.
    private Indexer _indexer;

    public PRFHandler(Options options, Indexer indexer)
    {
        _indexer = indexer;
    }

    private void respondWithMsg(HttpExchange exchange, final String message)  throws IOException
    {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
        OutputStream responseBody = exchange.getResponseBody();
        responseBody.write(message.getBytes());
        responseBody.close();
    }

    private void constructTextOutput(final List<PseudoRev.WPPair> terms, StringBuffer response)
    {
        for (PseudoRev.WPPair termProb : terms)
        {
            response.append(response.length() > 0 ? "\n" : "");
            response.append(termProb.term + "\t" + termProb.prob);
        }
        response.append(response.length() > 0 ? "\n" : "");
    }

    public void handle(HttpExchange exchange) throws IOException
    {
        String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equalsIgnoreCase("GET"))
        { // GET requests only.
            return;
        }

        // Print the user request header.
        Headers requestHeaders = exchange.getRequestHeaders();
        System.out.print("Incoming request to PRF HANDLER: ");
        for (String key : requestHeaders.keySet())
        {
            System.out.print(key + ":" + requestHeaders.get(key) + "; ");
        }
        System.out.println();

        // Validate the incoming request.
        String uriQuery = exchange.getRequestURI().getQuery();
        String uriPath = exchange.getRequestURI().getPath();
        if (uriPath == null || uriQuery == null)
        {
            respondWithMsg(exchange, "Something wrong with the URI!");
        }
        if (!uriPath.equals("/search") && !uriPath.equals("/prf"))
        {
            respondWithMsg(exchange, "Only /search is handled!");
        }
        System.out.println("Query: " + uriQuery);

    // Process the CGI arguments.
        //import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
        QueryHandler.CgiArguments cgiArgs = new QueryHandler.CgiArguments(uriQuery);
        if (cgiArgs._query.isEmpty())
        {
            respondWithMsg(exchange, "No query is given!");
        }

        // Create the ranker.
        Ranker ranker = Ranker.Factory.getRankerByArguments(
                cgiArgs, SearchEngine.OPTIONS, _indexer);
        if (ranker == null)
        {
            respondWithMsg(exchange,
                    "Ranker " + cgiArgs._rankerType.toString() + " is not valid!");
        }

        // Processing the query.
        QueryPhrase processedQuery = new QueryPhrase(cgiArgs._query);
        //Query processedQuery = new Query(cgiArgs._query);
        processedQuery.processQuery();

        // Ranking.
        Vector<ScoredDocument> scoredDocs = ranker.runQuery(processedQuery, cgiArgs._numResults);
        
        int K = cgiArgs._prf_numdocs;
        int m = cgiArgs._prf_numterms;
        List<WPPair> termProbList = (new PseudoRev(SearchEngine.OPTIONS)).computePRF(scoredDocs, K, m);
        
        //System.out.println(scoredDocs.size());
        StringBuffer response = new StringBuffer();
        switch (cgiArgs._outputFormat)
        {
            case TEXT:
                constructTextOutput(termProbList, response);
                break;
            case HTML:
                // @CS2580: Plug in your HTML output
                break;
            default:
            // nothing
        }
        respondWithMsg(exchange, response.toString());
        System.out.println("Finished query: " + cgiArgs._query);
    }
}
