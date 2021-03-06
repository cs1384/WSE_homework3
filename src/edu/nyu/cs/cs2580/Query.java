package edu.nyu.cs.cs2580;

import java.util.Scanner;
import java.util.Vector;

/**
 * Representation of a user query.
 * 
 * In HW1: instructors provide this simple implementation.
 * 
 * In HW2: students must implement {@link QueryPhrase} to handle phrases.
 * 
 * @author congyu
 * @auhtor fdiaz
 */
public class Query {
  public String _query = null;
  public Vector<String> _tokens = new Vector<String>();
private Stemmer stemmer = new Stemmer();

  public Query(String query) {
    _query = query;
  }

  public void processQuery() {
    if (_query == null) {
      return;
    }
    Scanner s = new Scanner(_query);
    while (s.hasNext()) {
        String token = s.next();
        stemmer.add(token.toCharArray(), token.length());
        stemmer.stem();
        token = stemmer.toString();
        
      _tokens.add(token);
    }
    s.close();
  }
}
