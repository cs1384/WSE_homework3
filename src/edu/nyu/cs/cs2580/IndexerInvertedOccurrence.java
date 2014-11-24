package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import edu.nyu.cs.cs2580.SearchEngine.Options;

import java.io.File;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable {
  private static final long serialVersionUID = 1077111905740085032L;
  
  private class Record implements Serializable{
    int lineN = -1;
    int fre = 0;
    public Record(int l, int f){
      lineN = l;
      fre = f;
    }
  }
  private Map<String, Record> _index =
      new HashMap<String, Record>();
  
  private class Posting implements Serializable{
    public int did;
    //get occurance by offsets.size()
    public Vector<Integer> offsets = new Vector<Integer>();
    public Posting(int did){
      this.did = did;
    }
  }
  private Map<String, Vector<Posting>> _op = 
      new TreeMap<String, Vector<Posting>>();
  
  private Map<String, Vector<Posting>> _working = 
          new HashMap<String, Vector<Posting>>();
  
  //map url to docid to support documentTermFrequency method,optional 
  //private Map<String, Integer> _urlToDoc = new HashMap<String, Integer>(); 
  
  private short _indexFileN = 0;
  //to store and quick access to basic document information such as title 
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  private int _uniqueTerms = 0;
  
  //to record term frequency for each doc
  private BufferedWriter docTermFreqWriter;
  private StopWords stopWords;
  
  //Provided for serialization
  public IndexerInvertedOccurrence() { }
  //constructor
  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }
  
  @Override
  public void constructIndex() throws IOException
  {
    printRuntimeInfo("======= Constructing =======");
    
    //to record term frequency for each doc
    try{
        docTermFreqWriter = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/dft_list_occurance")));
    }catch(Exception e){
        System.out.println("Failed to open doc-term-freq writer");
    }
    stopWords = new StopWords(_options);
    
    int batch = 0;
    try{
      String corpusFolder = _options._corpusPrefix + "/";
      System.out.println("Construct index from: " + corpusFolder);
      File folder = new File(corpusFolder);
      ArrayList<File> fileList = new ArrayList<File>();
      for (final File file : folder.listFiles()){
        fileList.add(file);
      }
      
      int lower=0, upper = 250;
      while(lower < fileList.size()){
        //System.out.println("range:  low = " + lower + " , upper = " + upper);
        if(upper > fileList.size()){
          upper = fileList.size();
        }
        constructPartialIndex(fileList.subList(lower, upper),batch++);
        _op.clear();
        lower = upper;
        upper += 250;
      } 
      //finalConstruction(batch);
    }catch (Exception e){
        e.printStackTrace();
    }
    docTermFreqWriter.close();
    docTermFreqWriter = null;
    
    printRuntimeInfo("======= Merge =======");
    
    try{
      File f = new File(_options._indexPrefix + "/merged_0.txt");
      f.createNewFile();            
      for(int i=0;i<batch;i++){
        mergeIndices(i) ;
        //System.out.println("Merged " + (i+1) + " / " + batch);
      }
      f = new File(_options._indexPrefix + "/merged_"+batch+".txt");
      f.renameTo(new File(_options._indexPrefix + "/Occurance_Index.txt"));
    }
    catch(Exception e)
    {
        e.printStackTrace();;
    }
    //makeIndex();
    
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
    
    if(this._corpusAnalyzer==null) System.out.println("test");

    String indexFile = _options._indexPrefix + "/OccuranceIndexer.idx";
    System.out.println("Store index to: " + indexFile);
    ObjectOutputStream writer
        = new ObjectOutputStream(new FileOutputStream(indexFile));
    writer.writeObject(this); //write the entire class into the file
    writer.close();
      
    printRuntimeInfo("======== END!! ========");
  }
  
  public void mergeIndices(int id){
    System.out.println("Merging file " + id);
    try{
      BufferedReader br1 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/index_" + id + ".txt")));
      BufferedReader br2 = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/merged_" + id + ".txt")));
      BufferedWriter outBw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/merged_" + (id+1) + ".txt")));
          
      //now walk the files, and write to a new file
      //int i=1, j=1;
      String file1Line = br1.readLine();
      String file2Line = br2.readLine();
      while(file1Line != null && file2Line != null){
        String tokens1[] = file1Line.split(" ");
        String tokens2[] = file2Line.split(" ");

        if(tokens1[0].compareTo(tokens2[0]) < 0){
          outBw.write(file1Line + "\n");
          file1Line = br1.readLine();
          //i++;
        }else if(tokens1[0].compareTo(tokens2[0]) > 0){
          outBw.write(file2Line + "\n");
          file2Line = br2.readLine();
          //j++;
        }else{
          StringBuilder sb = new StringBuilder();
          sb.append(file2Line);
          for(int k=1;k<tokens1.length;k++){
            sb.append(" ").append(tokens1[k]);
          }
          sb.append("\n");
          outBw.write(sb.toString());
          file1Line = br1.readLine();
          file2Line = br2.readLine();
        }
      }

      while(file1Line != null){
        outBw.write(file1Line + "\n");
        file1Line = br1.readLine();
      }
      while(file2Line != null){
        outBw.write(file2Line + "\n");
        file2Line = br2.readLine();
        //j++;                
      }
      outBw.close();
      br1.close();
      br2.close();
        
      //delete
      File f1 = new File(_options._indexPrefix + "/index_" + id + ".txt");
      File f2 = new File(_options._indexPrefix + "/merged_" + id + ".txt");
      
      f1.delete();
      f2.delete();
    }catch(Exception e){
          e.printStackTrace();
    }
  }
  
  private void constructPartialIndex(List<File> listOfFiles, int batch){
    try{
      for (File file : listOfFiles){
        String text = TestParse2.getPlainText(file);
        String title = file.getName();
        processDocument(text, title);
      }
    }catch(Exception e){
      e.printStackTrace();
    }

    try{
      Set<String> keys = _op.keySet();     
      BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/index_" + batch + ".txt")));
      StringBuilder sb = new StringBuilder();
      
      for(String term : keys){
        Vector<Posting> pv = _op.get(term);
        sb.append(term).append(" ");
        for(Posting p : pv){
          sb.append(p.did).append(" ");
          sb.append(p.offsets.size()).append(" ");
          for(Integer o : p.offsets){
            sb.append(o).append(" ");
          }
        }
        sb.append("\n");
      }
      bw.write(sb.toString());
      bw.close();
      System.out.println("Partially Indexed " + Integer.toString(_numDocs) + " docs with " + Long.toString(_totalTermFrequency) + " terms.");
    }catch(Exception e){
      e.printStackTrace();;
    }
  }

  public void makeIndex(){
    try{
      this._indexFileN = 0;
      BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/Occurance_Index.txt")));
      BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/Occurance_Index_"+this._indexFileN+".txt")));
      System.out.println("Writing /Occurance_Index_"+this._indexFileN+".txt");
      String line = br.readLine();
      int lineN = 1;
      while(line!=null){
        bw.write(line);
        bw.write("\n");
        if(lineN%10000==0){
          this._indexFileN++;
          bw.close();
          bw = new BufferedWriter(new FileWriter(new File(_options._indexPrefix + "/Occurance_Index_"+this._indexFileN+".txt")));
          System.out.println("Writing /Occurance_Index_"+this._indexFileN+".txt");
        }
        lineN++;
        line = br.readLine();
      }
      bw.close();
      br.close();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //File f = new File(_options._indexPrefix + "/Occurance_Index.txt");
    //f.delete();
  }
  
  public void buildIndex(){
    int lineN = 1;
    int i = 0;
    //this._indexFileN = 52; //test purpose
    while(i<=this._indexFileN){
      lineN = readFile(i,lineN);
      i++;
    }
  }
  
  public int readFile(int i, int lineN){
    Scanner sc;
    Scanner scl;
    try{
      //this.printRuntimeInfo("=======check========");
      System.out.println("Scanning /Occurance_Index_"+i+".txt...");
      sc = new Scanner(new File(_options._indexPrefix + "/Occurance_Index_"+i+".txt"));
      //BufferedReader br = new BufferedReader(new FileReader(new File(_options._indexPrefix + "/Occurance_Index_"+i+".txt")));
      sc.useDelimiter(System.getProperty("line.separator")); 
      int j, top, fre;
      while(sc.hasNext()){
        fre = 0;
        scl = new Scanner(sc.next());
        String term = scl.next();
        while(scl.hasNextInt()){
          scl.nextInt();// docid
          //System.out.println(sc.nextInt()); // docid
          top = scl.nextInt();
          //System.out.println(top);
          fre += top;
          for(j=0;j<top;j++){
            scl.nextInt();
            //System.out.println(sc.next());
          }
        }
        scl.close();
        _index.put(term,new Record(lineN,fre));
        lineN++;
      }
      sc.close();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.gc();
    return lineN;
  }
  
  public void processDocument(String content, String filename){
    
    //docid starts from 1
    DocumentIndexed doc = new DocumentIndexed(_documents.size() + 1);
    
    //doc.setTitle(filename.replace('_', ' '));
    doc.setTitle(filename);
    doc.setSize(ProcessTerms(content, doc._docid));
    //int numViews = (int) (Math.random() * 10000);
    //doc.setNumViews(numViews);

    String url = "en.wikipedia.org/wiki/" + filename;
    doc.setUrl(url);
    //build up urlToDoc map
    //_urlToDoc.put(filename, doc._docid);
    _documents.add(doc);
    _numDocs++;
    return;
  }
  
  @SuppressWarnings("unchecked")
public int ProcessTerms(String content, int docid){
    Map<String, Integer> docTermFreq = new HashMap<String, Integer>();
    Stemmer stemmer = new Stemmer();
    int offset = 1; //offset starts from 1
    Scanner s = new Scanner(content);
    
    while (s.hasNext()) {
      String token = s.next();
      stemmer.add(token.toCharArray(), token.length());
      stemmer.stem();
      token = stemmer.toString().toLowerCase();
            
      if(!stopWords.wordInList(token)){
          if(docTermFreq.containsKey(token))
              docTermFreq.put(token, docTermFreq.get(token) + 1);
          else
              docTermFreq.put(token, 1);
      }
      
      if(_op.containsKey(token)){
        if(_op.get(token).lastElement().did==docid){
          _op.get(token).lastElement().offsets.add(offset);
        }else{
          Vector<Integer> offsetTracker = new Vector<Integer>();
          offsetTracker.add(offset);
          Posting posting = new Posting(docid);
          posting.offsets = offsetTracker;
          _op.get(token).add(posting);
        }
      }else{
        Vector<Integer> offsetTracker = new Vector<Integer>();
        offsetTracker.add(offset);
        Posting posting = new Posting(docid);
        posting.offsets = offsetTracker;
        Vector<Posting> list = new Vector<Posting>();
        list.add(posting);
        _op.put(token, list);
      }
      offset++;
      _totalTermFrequency++;
    }
    s.close();
    
    //sort the dtf map
    List dtfList = new ArrayList(docTermFreq.entrySet());
    Collections.sort(dtfList, new Comparator(){
        public int compare(Object o1, Object o2){
           return -1*((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
        }
    });
    //Write the first 10 terms in to the file
    //format: first entry is docid, subsequent are the most frequent words
    try{
        for(int ii=0;ii<dtfList.size();ii++){
            Entry<String, Integer> entry = (Entry<String, Integer>) dtfList.get(ii);
            String word = entry.getKey();
            docTermFreqWriter.write(word + " ");
            docTermFreqWriter.write(entry.getValue() + " ");
        }
        docTermFreqWriter.write("\n");
    }catch(Exception e){
        System.out.println("Failed to write to dtf file");
        e.printStackTrace();
    }
    
    return offset-1;
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    this.printRuntimeInfo("======== Loading =========");
    String indexFile = _options._indexPrefix + "/OccuranceIndexer.idx";
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedOccurrence loaded = 
        (IndexerInvertedOccurrence) reader.readObject();
    
    this._documents = loaded._documents;
    this._index = loaded._index;
    this._indexFileN = loaded._indexFileN;
    System.out.println(this._indexFileN);
    makeIndex();
    buildIndex();
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this._totalTermFrequency = 0;
    for (Record rc : _index.values()) {
      this._totalTermFrequency += rc.fre;
    }
    this._op = loaded._op;
    
    this._corpusAnalyzer.load();
    for(Document doc : this._documents){
        doc.setPageRank(this._corpusAnalyzer.getRank(doc.getTitle()));
        //System.out.println(doc.getTitle() + ", " + doc.getPageRank());
    }
    this._corpusAnalyzer = null;
    
    
    this._logMiner.load();
    for(Document doc : this._documents){
        doc.setNumViews(((LogMinerNumviews)this._logMiner)._numViews.get(doc.getTitle()));
        System.out.println(doc.getTitle() + ", " + doc.getNumViews());
    }
    
    reader.close();
    this.printRuntimeInfo("======== Done =========");
    System.out.println(Integer.toString(_numDocs) + " documents loaded " +
        "with " + Long.toString(_totalTermFrequency) + " terms!");
    
  }

  @Override
  public Document getDoc(int docid) {
    return (docid > _documents.size() || docid <= 0) ? 
        null : _documents.get(docid-1);
  }

  public void setupWorkingMap(Query query){
      _working.clear();
      for(String term : query._tokens){
          _working.put(term, getPostingList(term));
      }
      if(query instanceof QueryPhrase){
          for(Vector<String> phrase : ((QueryPhrase) query)._phrases){
              for(String term : phrase){
                  _working.put(term, getPostingList(term));
              }
          }
      }
  }
  
  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   */
  public Document nextDoc(Query query, int docid) {
        
      setupWorkingMap(query);  
      boolean keep = false;
      int did = docid;
    
      //keep getting document until no next available 
      while((did = nextDocByTerms(query._tokens,did))!=Integer.MAX_VALUE){
          System.out.println("checking page : "+ did);
          keep = false;
          //check if the resulting doc contains all phrases 
          if(query instanceof QueryPhrase){
              for(Vector<String> phrase : ((QueryPhrase)query)._phrases){
                  //if not, break the for loop and get next doc base on tokens
                  int temp = nextPositionByPhrase(phrase,did,-1);
                  if(temp==Integer.MAX_VALUE){
                      keep = true;
                      break;
                  }
              }
          }
          if(keep){
              continue;
          }else{
              //create return object if passed all phrase test and return
              DocumentIndexed result = this._documents.get(did-1); 
              return result;
          }
      }
      //no possible doc available
      return null;
  }
  
  public int nextDocByTerms(Vector<String> terms, int curDid){
      if(terms.size()<=0){
          if(curDid<=0){
              return 1;
          }else if(curDid>=_numDocs){
              return Integer.MAX_VALUE;
          }else{
              return curDid+1;
          }
      }
      int did = nextDocByTerm(terms.get(0), curDid);
      boolean returnable = true;
      int largestDid = did;
      int i = 1;
      int tempDid;
      for(;i<terms.size();i++){
          tempDid = nextDocByTerm(terms.get(i), curDid);
          //one of the term will never find next
          if(tempDid==Integer.MAX_VALUE){
              return Integer.MAX_VALUE;
          }
          if(tempDid>largestDid){
              largestDid = tempDid;
          } 
          if(tempDid!=did){
              returnable = false;
          }
      }    
      if(returnable){
          return did;
      }else{
          return nextDocByTerms(terms, largestDid-1);
      }
  }
  public int nextDocByTerm(String term, int curDid){
      Vector<Posting> op = _working.get(term);
      if(op.size()>0){
          int largest = op.lastElement().did;
          if(largest <= curDid){
              return Integer.MAX_VALUE;
          }
          if(op.firstElement().did > curDid){
              return op.firstElement().did;
          }
          return binarySearchDoc(op,0,op.size()-1,curDid);
      }else{
          return Integer.MAX_VALUE;
      }
  }
  public int binarySearchDoc(Vector<Posting> op, int low, int high, int curDid){
      int mid;
      while((high-low)>1){
        mid = (low+high)/2;
        if(op.get(mid).did <= curDid){
          low = mid;
        }else{
          high = mid;
        }
      }
      return op.get(high).did;
  }
  
  public int nextPositionByPhrase(Vector<String> phrase, int docid, int pos){
    int did = nextDocByTerms(phrase, docid-1);
    if(docid != did){
      return Integer.MAX_VALUE;
    }
    int position = nextPositionByTerm(phrase.get(0), docid, pos);
    boolean returnable = true;
    int largestPos = position;
    int i = 1;
    int tempPos;
    for(;i<phrase.size();i++){
      tempPos = nextPositionByTerm(phrase.get(i), docid, pos);
      //one of the term will never find next
      if(tempPos==Integer.MAX_VALUE){
        return Integer.MAX_VALUE;
      }
      if(tempPos>largestPos){
        largestPos = tempPos;
      } 
      if(tempPos!=position+1){
        returnable = false;
      }else{
        position = tempPos;
      }
    }    
    if(returnable){
      return position;
    }else{
      return nextPositionByPhrase(phrase, docid, largestPos);
    }
  }
  public int nextPositionByTerm(String term, int docid, int pos){
    Vector<Posting> list = _working.get(term);
    //System.out.println("size"+list.size());
    if(list.size()>0){
      Posting op = binarySearchPosting(list, 0, list.size()-1, docid);
      if(op==null){
        return Integer.MAX_VALUE; 
      }
      int largest = op.offsets.lastElement();
      if(largest <= pos){
        return Integer.MAX_VALUE;
      }
      if(op.offsets.firstElement() > pos){
        return op.offsets.firstElement();
      }
      return binarySearchOffset(op.offsets,0,op.offsets.size(),pos);
    }
    return Integer.MAX_VALUE;
  }
  public Posting binarySearchPosting(
          Vector<Posting> list, int low, int high, int docid){
      int mid;
      while((high-low)>1){
          mid = (low+high)/2;
          if(list.get(mid).did < docid){
            low = mid;
          }else{
            high = mid;
          }
      }
      if(list.get(high).did==docid){
          return list.get(high);
      }else{
          return list.get(low);
      }
  }
  public int binarySearchOffset(Vector<Integer> offsets, int low, int high, int pos){
    int mid;
    while((high-low)>1){
      mid = (low+high)/2;
      if(offsets.get(mid) <= pos){
        low = mid;
      }else{
        high = mid;
      }
    }
    return offsets.get(high);
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    Vector<Posting> temp = getPostingList(term);
    return temp.size();
  }
  
  public Vector<Posting> getPostingList(String term){
    Vector<Posting> result = new Vector<Posting>();
    
    Stemmer stemmer = new Stemmer(); 
    stemmer.add(term.toCharArray(), term.length());
    stemmer.stem();
    term = stemmer.toString().toLowerCase();
    
    if(!_index.containsKey(term)){
      return result;
    }else{
      int lineN = _index.get(term).lineN;
      //System.out.println("lineN:" + lineN);
      try {
        int fileN = lineN/10001;
        lineN = lineN%10000;
        File file = new File(_options._indexPrefix + "/Occurance_Index_"+fileN+".txt");
        String line = FileUtils.readLines(file).get(lineN-1).toString();
        //System.out.println(line);
        StringTokenizer st = new StringTokenizer(line); 
        int i,offsetN;
        st.nextToken(); //term
        while(st.hasMoreTokens()){
          Posting posting = new Posting(Integer.parseInt(st.nextToken()));
          offsetN = Integer.parseInt(st.nextToken());
          for(i=0;i<offsetN;i++){
            posting.offsets.add(Integer.parseInt(st.nextToken()));
          }
          result.add(posting);
        }
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return result;
    }
  }

  @Override
  public int corpusTermFrequency(String term) {
    int result = 0;
    Vector<Posting> op = getPostingList(term);
    for(Posting ps : op){
      result += ps.offsets.size();
    }
    return result;
  }

  @Override
  public int documentTermFrequency(String term, int docid){
      return -1;
  }
  
  
  public int documentTermFrequency(String term, String url) {
    Vector<Posting> op = this.getPostingList(term);
    for(int i=0;i<op.size();i++){
      if(_documents.get(op.get(i).did-1).getUrl().equals(url)){
        return op.get(i).offsets.size();
      }
    }
    return 0;
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
  
  public static void main(String args[]){    
    try {
      Options options = new Options("conf/engine.conf");
      IndexerInvertedOccurrence a = new IndexerInvertedOccurrence(options);
      //a.constructIndex();
      a.loadIndex();
      //a.getPostingList("zatanna");
      
      //System.out.println(a._index.containsKey("google"));
      //System.out.println(a._index.containsKey("north"));
      //QueryPhrase q11 = new QueryPhrase("which");
      QueryPhrase q12 = new QueryPhrase("\"north american\" any");
      QueryPhrase q13 = new QueryPhrase("\"web searching\" google");
      //DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, -1);
      //System.out.println(d11._docid);
      //DocumentIndexed d12 = (DocumentIndexed) a.nextDoc(q12, -1);
      //System.out.println(d12.getUrl());
      DocumentIndexed d13 = (DocumentIndexed) a.nextDoc(q13, -1);
      System.out.println(d13.getUrl());
      /*
      QueryPhrase q11 = new QueryPhrase("kicktin");
      DocumentIndexed d11 = (DocumentIndexed) a.nextDoc(q11, -1);
      System.out.println(d11._docid);
      //System.out.println(a.nextDocByTerm("kicktin", -1));
      */
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }
}
