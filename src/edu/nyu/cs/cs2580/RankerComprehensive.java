package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3 based on your {@code RankerFavorite}
 * from HW2. The new Ranker should now combine both term features and the
 * document-level features including the PageRank and the NumViews. 
 */
public class RankerComprehensive extends Ranker {
    private double lambda = 0.5;
    
  public RankerComprehensive(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
      try{
          QueryPhrase qp = new QueryPhrase(query._query);
          
          if(this._indexer instanceof IndexerInvertedOccurrence){
              Vector<ScoredDocument> all = new Vector<ScoredDocument>();
              DocumentIndexed doc = (DocumentIndexed) this._indexer.nextDoc(qp, -1);
              while(doc!=null){
                  all.add(scoreDocument(qp,doc._docid));
                  doc = (DocumentIndexed) this._indexer.nextDoc(qp, doc._docid);
              }
              Collections.sort(all, Collections.reverseOrder());
              Vector<ScoredDocument> results = new Vector<ScoredDocument>();
              for (int i = 0; i < all.size() && i < numResults; ++i) {
                results.add(all.get(i));
              }
              return results;
          }else{
              Vector<ScoredDocument> retrievedDocs = new Vector<ScoredDocument>();
              if(qp._tokens.size() > 0){
                  Document d = this._indexer.nextDoc(qp, 0);
                  while(d != null){
                      ScoredDocument sd = scoreDocument(qp, d._docid);
                      retrievedDocs.add(sd);
                      d = this._indexer.nextDoc(qp, d._docid);
                  }
              }
              
              Set<DocumentIndexed> finalSet = new HashSet<DocumentIndexed>();
              for(int i=0;i<qp._phrases.size();i++){
                  try{
                      Vector<DocumentIndexed> vec = ((IndexerInvertedCompressed) _indexer).allDocPhrase(qp._phrases.get(i));
                      if(i > 0)
                          finalSet.retainAll(vec);
                      else
                          finalSet.addAll(vec);
                  }catch(Exception e){
                      e.printStackTrace();
                  }
              }
              
              for(DocumentIndexed d : finalSet){
                  ScoredDocument sd = scoreDocument(qp, d._docid);
                  retrievedDocs.add(sd);
              }
              System.out.println("Got all docs");
              Collections.sort(retrievedDocs, Collections.reverseOrder());
              return retrievedDocs;
          }
      }catch(Exception e){
          e.printStackTrace();
      }
      
      return null;
  }
  
    public void setLambda(double lambda){
      this.lambda = lambda;
    }
    
    public double getLambda(){
      return this.lambda;
    }
  
  private ScoredDocument scoreDocument(Query query, int did) {

      DocumentIndexed doc = (DocumentIndexed) _indexer.getDoc(did);
      if(doc == null)
            System.out.println("Null doc");
      
      float relevance = 0.0f;
      for(int i = 0;i<query._tokens.size();i++){
        String str = query._tokens.get(i);
        int freq = _indexer.documentTermFrequency(str, doc._docid);
        relevance += Math.log10(
            (1-lambda)
            *freq
            / ((DocumentIndexed)doc).getSize() 
            + lambda
            * _indexer.corpusTermFrequency(str)
            /_indexer._totalTermFrequency);
      }
      if(query instanceof QueryPhrase){
          for(int i = 0;i<((QueryPhrase)query)._phrases.size();i++){
              for(int j=0;j<((QueryPhrase)query)._phrases.get(i).size();j++){
                  String str = ((QueryPhrase)query)._phrases.get(i).get(j);
                  int freq = _indexer.documentTermFrequency(str, doc._docid);
                  relevance += Math.log10(
                          (1-lambda)
                          *freq
                          / ((DocumentIndexed)doc).getSize() 
                          + lambda
                          * _indexer.corpusTermFrequency(str)
                          /_indexer._totalTermFrequency);
              }
              
          }
      }
      relevance = (float) Math.pow(10,relevance);
      
      float pagerank = doc.getPageRank();
      int numview = doc.getNumViews();
      double score = 0.5 * relevance + 0.25 * pagerank + 0.25 * numview;
      
      return new ScoredDocument(doc, score);
      
    }
}
