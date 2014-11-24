package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class LogMinerNumviews extends LogMiner implements Serializable{

    public class Track implements Serializable{
        private static final long serialVersionUID = 1L;
        int avg = 0;
        int count = 0;
        int sum = 0;
    }
    
    private static final long serialVersionUID = 1077111905740085033L;
    protected Map<String, Integer> _numViews = new HashMap<String, Integer>();
    protected Track track = new Track();
    
  public LogMinerNumviews(Options options) {
    super(options);  
  }

  public LogMinerNumviews() {super();}

/**
   * This function process  s the logs within the log directory as specified by
   * the {@link _options}. The logs are obtained from Wikipedia dumps and have
   * the following format per line: [language]<space>[article]<space>[#views].
   * Those view information are to be extracted for documents in our corpus and
   * stored somewhere to be used during indexing.
   *
   * Note that the log contains view information for all articles in Wikipedia
   * and it is necessary to locate the information about articles within our
   * corpus.
 * @throws FileNotFoundException 
   *
   * @throws IOException
   */
  @Override
  public void compute() throws IOException{
    System.out.println("Computing using " + this.getClass().getName());
    
    /*Initialize the numview of wiki docs*/
    
    File corpus_folder = new File(_options._corpusPrefix);
    for(File file_name: corpus_folder.listFiles()){
        _numViews.put(file_name.getName(), 0);
    }
    
    /*Read the log, update the numview*/
    FileReader file_log;
    try {
        file_log = new FileReader(_options._logPrefix+"/20140601-160000.log");
        BufferedReader buffer_reader = new BufferedReader(file_log);
        String line = buffer_reader.readLine();
        String[] tmp;
        Integer view_count = 0;
        
        Integer sum = 0;
        Integer count = 0;
        while(line != null){
                tmp = line.trim().split(" ");
                if (_numViews.get(tmp[1])!=null && tmp.length==3 && isInt(tmp[2])){
                    view_count = Integer.parseInt(tmp[2]);
                    sum += Integer.parseInt(tmp[2]);
                    _numViews.put(tmp[1], view_count);
                    count++;
                    if(count%500==0)
                        System.out.println("scanned " + count + " pages");
                }
                line = buffer_reader.readLine();
        }
        buffer_reader.close();
        
        
        //System.out.println("sum:"+sum);
        //System.out.println("count"+count);
        this.track.count = count;
        this.track.sum = sum;
        count /= 2;
        //System.out.println(sum/count);
        this.track.avg = sum/count;
        System.out.println("Average:"+this.track.avg);
        
        /*
        for(String s : this._numViews.keySet()){
            float score = (float)_numViews.get(s)/avg >1?1:(float)_numViews.get(s)/avg;
            _numViews.put(s, score);
        }
        */
        
        /*Output to the file*/
        String numviewsFile = _options._indexPrefix + "/numViews.idx";
        System.out.println("Store numviews to: " + numviewsFile);
        File file = new File(_options._indexPrefix);
        file.mkdirs();
        ObjectOutputStream writer
            = new ObjectOutputStream(new FileOutputStream(numviewsFile));
        writer.writeObject(this); //write the entire class into the file
        writer.close();
        return;
    } catch (FileNotFoundException e) {
        System.out.println("log file doesn't exist");
        e.printStackTrace();
    }
   
  }
  
  
  public boolean isInt(String s){
	  try{
		  int i = Integer.parseInt(s);
		  return true;
	  }catch(Exception e){
		  return false;
	  }
  }

  /**
   * During indexing mode, this function loads the NumViews values computed
   * during mining mode to be used by the indexer.
   * 
   * @throws IOException
   */
  @Override
  public Object load() throws IOException {
      
      System.out.println("Loading using " + this.getClass().getName());
      
      String indexFile = _options._indexPrefix + "/numViews.idx";
      System.out.println("Load numviews from: " + indexFile);
      
      try {
      ObjectInputStream reader =
              new ObjectInputStream(new FileInputStream(indexFile));
      LogMinerNumviews loaded = 
              (LogMinerNumviews) reader.readObject();
          
          this._numViews = loaded._numViews;
          this._options = loaded._options;
          this.track = loaded.track;
          reader.close();
          
      } catch (ClassNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      return null;
  }
  
  public Object load(String path) throws IOException {
      
      System.out.println("Loading using " + this.getClass().getName());      
      System.out.println("Load numviews from: " + path);
      
      try {
      ObjectInputStream reader =
              new ObjectInputStream(new FileInputStream(path));
      LogMinerNumviews loaded = 
              (LogMinerNumviews) reader.readObject();
          
          this._numViews = loaded._numViews;
          this._options = loaded._options;
          this.track = loaded.track;
          reader.close();
          
      } catch (ClassNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      return null;
  }
  
  
  public static void main(String[] args) throws IOException{
      Options options = new Options("conf/engine.conf");
      LogMinerNumviews lm = new LogMinerNumviews(options);
      lm.compute();
      lm.load();
      System.out.println(lm.track.avg);
      System.out.println("===== first 10 result =====");
      int count = 0;
      for(String s : lm._numViews.keySet()){
          if(count>=10)
              break;
          System.out.println(s + ", " + lm._numViews.get(s));
          count++;
      }
  }
  
}