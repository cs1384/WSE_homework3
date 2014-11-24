package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer implements Serializable{
    private static final long serialVersionUID = 1077111905740085032L;
    protected Map<String, Integer> _fileN = new HashMap<String, Integer>();
    protected PageInfo[] _op;
    protected float[] _rank;
    protected String _idxPath = null;
    
    
    private float lambda = 0.5f; 
    private int iterationTimes = 1;
    
    public class PageInfo implements Serializable{
        int linkN = 0;
        LinkedList<Integer> fromPages = new LinkedList<Integer>();
    }
    
  //Provided for serialization
  public CorpusAnalyzerPagerank(){ super();}
    
  public CorpusAnalyzerPagerank(Options options) {
    super(options);
  }
  
  public void setIdxPath(String path){
      this._idxPath = path;
  }

  /**
   * This function processes the corpus as specified inside {@link _options}
   * and extracts the "internal" graph structure from the pages inside the
   * corpus. Internal means we only store links between two pages that are both
   * inside the corpus.
   * 
   * Note that you will not be implementing a real crawler. Instead, the corpus
   * you are processing can be simply read from the disk. All you need to do is
   * reading the files one by one, parsing them, extracting the links for them,
   * and computing the graph composed of all and only links that connect two
   * pages that are both in the corpus.
   * 
   * Note that you will need to design the data structure for storing the
   * resulting graph, which will be used by the {@link compute} function. Since
   * the graph may be large, it may be necessary to store partial graphs to
   * disk before producing the final graph.
   *
   * @throws IOException
   */
  @Override
  public void prepare() throws IOException {
    System.out.println("Preparing " + this.getClass().getName());
    printRuntimeInfo("===== start preparing! =====");
    
    File folder = new File(this._options._corpusPrefix);
    ArrayList<File> fileList = new ArrayList<File>();
    for (final File file : folder.listFiles()){
        if(isValidDocument(file)){
            fileList.add(file);
        }
    }
    
    int n = 0;
    for(final File file : fileList){
        _fileN.put(file.getName(), n++);
    }
    
    _op = new PageInfo[n];
    int count = 1;
    for(final File file: fileList){
        if(count%500==0)
            System.out.println("processed " + count + " pages");
        processDocument(file);
        count++;
    }
    
    printRuntimeInfo("===== finish preparing! =====");
    System.out.println(this._op.length + " pages scanned!");
    return;
  }
  
  public void processDocument(File file){
      int linkN = 0;
      try {
          //set up fromPage
          HeuristicLinkExtractor extractor = new HeuristicLinkExtractor(file);
          int from = _fileN.get(extractor.getLinkSource());
          if(_op[from]==null){
              _op[from] = new PageInfo();
          }
          Set<String> uniqueLinks = new HashSet<String>();
          //find the link in fromPage and update the PageInfo
          String toPage = extractor.getNextInCorpusLinkTarget();
          while(toPage!=null){
              if(!uniqueLinks.contains(toPage) && 
                      _fileN.containsKey(toPage)){
                  uniqueLinks.add(toPage);
                  int to = _fileN.get(toPage);
                  if(_op[to]==null){
                      _op[to] = new PageInfo();
                  }
                  _op[to].fromPages.add(from);
                  linkN++;
              }
              toPage = extractor.getNextInCorpusLinkTarget();
          }
          _op[from].linkN = linkN;
      } catch (IOException e) {
          e.printStackTrace();
      } 
  }
  
  
  public void setLambda(float lambda){
      this.lambda = lambda;
  }
  public float getLambda(){
      return this.lambda;
  }
  public void setInterationTimes(int times){
      this.iterationTimes = times;
  }
  public int getInterationTimes(){
      return this.iterationTimes;
  }
  /**
   * This function computes the PageRank based on the internal graph generated
   * by the {@link prepare} function, and stores the PageRank to be used for
   * ranking.
   * 
   * Note that you will have to store the computed PageRank with each document
   * the same way you do the indexing for HW2. I.e., the PageRank information
   * becomes part of the index and can be used for ranking in serve mode. Thus,
   * you should store the whatever is needed inside the same directory as
   * specified by _indexPrefix inside {@link _options}.
   *
   * @throws IOException
   */
  @Override
  public void compute() throws IOException {
    System.out.println("Computing using " + this.getClass().getName());
    printRuntimeInfo("===== start computing! =====");
    
    if(_op==null){
        System.out.println("Prepare first!");
        return;
    }
    //initialize the _rank array
    _rank = new float[_op.length];
    for(int i=0;i<_rank.length;i++){
        _rank[i] = (float)1/_rank.length;
    }
    //iteration(s)
    for(int k=0;k<this.iterationTimes;k++){
        float[] newRank = new float[_rank.length];
        for(int i=0;i<_op.length;i++){
            float get = 0.0f;
            Iterator<Integer> it=_op[i].fromPages.iterator();
            int from;
            while(it.hasNext()){
                from = (int)it.next();
                get += (float)_rank[from]/_op[from].linkN;
            }
            if(_op[i].linkN==0){
                rankSink(i, newRank);
            }
            //System.out.println("get "+get);
            newRank[i] += (lambda/_op.length) + (1-lambda)*get;
        }
        //printTest(newRank);
        this._rank = newRank;
    }
    
    printRuntimeInfo("===== finish computing! =====");
    System.out.println(this._rank.length + " pages ranked!");
    
    String rankFile = _options._indexPrefix + "/pageRank.idx";
    System.out.println("Store pagerank to: " + rankFile);
    File file = new File(_options._indexPrefix);
    file.mkdirs();
    ObjectOutputStream writer
        = new ObjectOutputStream(new FileOutputStream(rankFile));
    writer.writeObject(this); //write the entire class into the file
    writer.close();
    return;
  }
  public void rankSink(int where, float[] a){
      float add = _rank[where]/(a.length-1)*lambda;
      for(int i=0;i<a.length;i++){
          if(i!=where){
              a[i] += add;
          }
      }
  }

  /**
   * During indexing mode, this function loads the PageRank values computed
   * during mining mode to be used by the indexer.
   *
   * @throws IOException
   */
  @Override
  public Object load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    this.printRuntimeInfo("======== start loading =========");
    
    String indexFile = _options._indexPrefix + "/pageRank.idx";
    System.out.println("Load pagerank from: " + indexFile);
    
    try {
    ObjectInputStream reader =
            new ObjectInputStream(new FileInputStream(indexFile));
        CorpusAnalyzerPagerank loaded = 
            (CorpusAnalyzerPagerank) reader.readObject();
        
        this._fileN = loaded._fileN;
        this._op = loaded._op;
        this._options = loaded._options;
        this._rank = loaded._rank;
        this.iterationTimes = loaded.iterationTimes;
        this.lambda = loaded.lambda;
        reader.close();
        
    } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    
    /*
    for(String s : this._fileN.keySet()){
        System.out.println(s + ", " + _rank[_fileN.get(s)]);
    }
    */
    printRuntimeInfo("======== done loading =========");
    return null;
  }
  
  public Object load(String path) throws IOException {
      System.out.println("Loading using " + this.getClass().getName());
      this.printRuntimeInfo("======== start loading =========");
      
      System.out.println("Load pagerank from: " + path);
      
      try {
      ObjectInputStream reader =
              new ObjectInputStream(new FileInputStream(path));
          CorpusAnalyzerPagerank loaded = 
              (CorpusAnalyzerPagerank) reader.readObject();
          
          this._fileN = loaded._fileN;
          this._op = loaded._op;
          this._options = loaded._options;
          this._rank = loaded._rank;
          this.iterationTimes = loaded.iterationTimes;
          this.lambda = loaded.lambda;
          reader.close();
          
      } catch (ClassNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      /*
      for(String s : this._fileN.keySet()){
          System.out.println(s + ", " + _rank[_fileN.get(s)]);
      }
      */
      printRuntimeInfo("======== done loading =========");
      return null;
    }
  
  
  public float getRank(String docName){
      if(this._fileN.containsKey(docName)){
          return this._rank[this._fileN.get(docName)];
      }else{
          return -1;
      }
  }
 
  public void printTest(float[] n){
      for(int i=0;i<_op.length;i++){
          System.out.println("Page " + i + " has " + _op[i].linkN + " links");
          System.out.print(" and incoming links from ");
          Iterator<Integer> it = _op[i].fromPages.iterator();
          int index;
          while(it.hasNext()){
              index = (int)it.next();
              System.out.print(index + "(" + _rank[index] + "/" + _op[index].linkN + ") ");
          }
          System.out.println();
          System.out.println("which gets us " + n[i]);
          System.out.println();
      }
  }
  public void printRuntimeInfo(String msg){
      System.out.println();
      System.out.println(msg);
      System.out.println(new Date());
      int mb = 1024;
      Runtime rt = Runtime.getRuntime();
      System.out.print(rt.freeMemory()/mb + ", ");
      System.out.print(rt.totalMemory()/mb + ", ");
      System.out.print(rt.maxMemory()/mb + ", ");
      System.out.println("used " + (rt.totalMemory() - rt.freeMemory())/mb + "KB");
    }
  public static void main(String[] args) throws IOException{
      Options options = new Options("conf/engine.conf");
      CorpusAnalyzerPagerank ca = new CorpusAnalyzerPagerank(options);
      ca.prepare();
      ca.compute();
      ca.load();
      
      //System.out.println(ca._rank.length + " pages");
      System.out.println("===== first 10 result =====");
      float sum = 0.0f;
      int i = 0;
      for(String s : ca._fileN.keySet()){
          System.out.print(s + ", ");
          float rank = ca.getRank(s);
          sum += rank;
          System.out.println(rank);
          if(i>=10)
              break;
          i++;
      }
      System.out.println();
      System.out.println("sum: " + sum);
      System.out.println(ca._fileN.get("Blue_whale"));

  }
}
