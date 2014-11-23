package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Spearman {
    
    private CorpusAnalyzerPagerank _corpusAnalyzer = null;
    private LogMinerNumviews _logMiner = null;
    Map<Float, Integer> PRconvertor = new HashMap<Float, Integer>();
    
    protected Spearman(String PRpath, String NVpath){
        this._corpusAnalyzer = new CorpusAnalyzerPagerank();
        try {
            this._corpusAnalyzer.load(PRpath);
            PRconvertor.clear();
        } catch (IOException e) {
            System.out.println("Pagerank idx file doesn't exit");
            e.printStackTrace();
        }
        this._logMiner = new LogMinerNumviews();
    }
    public float getCorrelation(){
        float[] ranks = _corpusAnalyzer._rank;
        
        //if the number of docs processed by two approaches are different
        //if(ranks.length!=_logMiner.){
        if(ranks.length==_logMiner.hashCode()){
            System.out.println("Corpus size unmatched");
        }
        //build converter to get place for each raw score
        Arrays.sort(ranks);
        int place = 1;
        for(int i=ranks.length-1;i>=0;i--){
            //System.out.println(ranks[i]);
            if(!PRconvertor.containsKey(ranks[i])){
                PRconvertor.put(ranks[i], place++);
                //System.out.println("add!");
            }
            
        }
        //compute z
        float Znumerator = 0.0f;
        for(String docname : _corpusAnalyzer._fileN.keySet()){
            Znumerator += Xi(docname);
        }
        float z = Znumerator / ranks.length;
        //compute correlation
        float numerator = 0.0f;
        float denomLeft = 0.0f;
        float denomRight = 0.0f;
        float x, y;
        for(String docname : _corpusAnalyzer._fileN.keySet()){
            x = Xi(docname)-z;
            y = Yi(docname)-z;
            numerator += x*y;
            denomLeft += Math.pow(x, 2);
            denomRight += Math.pow(y, 2);
        }
        //System.out.println(numerator);
        //System.out.println(denomLeft);
        //System.out.println(denomRight);
        float denominator = (float) (Math.sqrt(denomLeft)*Math.sqrt(denomRight));
        return numerator/denominator;
    }
    
    private int Xi(String docname){
        int index = _corpusAnalyzer._fileN.get(docname);
        float raw = _corpusAnalyzer._rank[index];
        return PRconvertor.get(raw);
    }
    private int Yi(String docname){
        return 1;
    }
    
    public static void main(String[] args) {
        if(args.length<2){
            System.out.println("Not enough argument!");
        }else{
            Spearman spearman = new Spearman(args[0],args[1]);
            System.out.println(spearman.getCorrelation());
        }
    }

}
