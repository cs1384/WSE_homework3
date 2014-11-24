package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Spearman {
    
    private CorpusAnalyzerPagerank _corpusAnalyzer = null;
    private LogMinerNumviews _logMiner = null;
    Map<Float, Integer> PRconvertor = new HashMap<Float, Integer>();
    Map<String, Integer> numViews = new HashMap<String, Integer>();
	Map<String, Double> pageRank = new HashMap<String, Double>();
	private Map<String, Integer> numViews_Rank = new HashMap<String, Integer>();
	private Map<String, Integer> pageRank_Rank = new HashMap<String, Integer>();
    
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
        try {
            this._logMiner.load(NVpath);
        } catch (IOException e) {
            System.out.println("Numviews idx file doesn't exit");
            e.printStackTrace();
        }
    }
    
    
    public void load_doc_HashMap(){
    	/*Load the doc_name, page_rank*/
    	for(String docname : _corpusAnalyzer._fileN.keySet()){
            pageRank.put(docname, (double) _corpusAnalyzer.getRank(docname));
        }
    	numViews = _logMiner._numViews;
    	
    	pageRank_Rank = sort_map(pageRank);
    	
    	numViews_Rank = sort_map2(numViews);
    }
    
    public Map<String, Integer> sort_map(Map<String, Double> original_map){
		Map<String, Integer> sort_map = new HashMap<String, Integer>();
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(original_map.entrySet());
		
		Collections.sort(list,new Comparator<Map.Entry<String,Double>>(){
			public int compare(Map.Entry<String, Double> doc1, Map.Entry<String, Double> doc2) {
				if(doc2.getValue() > doc1.getValue()) {
					return 1;
				}else if(doc2.getValue() < doc1.getValue()){
					return -1;
				}else{
					return doc2.getKey().compareTo(doc1.getKey());
				}
			}	
		});
		
		int rank = 1;
		for(Map.Entry<String, Double> doc : list) {
			sort_map.put(doc.getKey(), rank);
			rank ++;
		}
		return sort_map;		
	}
    
    
    public Map<String, Integer> sort_map2(Map<String, Integer> original_map){
		Map<String, Integer> sort_map = new HashMap<String, Integer>();
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(original_map.entrySet());
		
		Collections.sort(list,new Comparator<Map.Entry<String,Integer>>(){
			public int compare(Map.Entry<String, Integer> doc1, Map.Entry<String, Integer> doc2) {
				if(doc2.getValue() > doc1.getValue()) {
					return 1;
				}else if(doc2.getValue() < doc1.getValue()){
					return -1;
				}else{
					return doc2.getKey().compareTo(doc1.getKey());
				}
			}	
		});
		
		int rank = 1;
		for(Map.Entry<String, Integer> doc : list) {
			sort_map.put(doc.getKey(), rank);
			rank ++;
		}
		return sort_map;		
	}
	
    public double compute_Spearman(Map<String, Integer> numViews_rank, Map<String, Integer> pageRank_rank){
		double x_k;
		double y_k;
		double up_cumulative = 0.0;
		Set<String> docs = numViews_rank.keySet();
		double n = docs.size();
		for(String doc: docs){
			if (numViews_rank.get(doc) != null){
				x_k = numViews_rank.get(doc);
			}else{
				x_k = 0.0;
			}
			
			if (pageRank_rank.get(doc) != null){
				y_k = pageRank_rank.get(doc);
			}else{
				y_k = 0.0;
			}
			
			up_cumulative += Math.pow(x_k - y_k, 2);
		}
		
		double rho = 1 - 6*up_cumulative/(Math.pow(n, 3) - n);
		return rho;
	}
    
    public static void main(String[] args) {
        if(args.length<2){
            System.out.println("Not enough argument!");
        }else{
            Spearman spearman = new Spearman(args[0],args[1]);
            spearman.load_doc_HashMap();
            int count = 0;
            for(String s : spearman._logMiner._numViews.keySet()){
                if(count>=10)
                    break;
                System.out.println(s + ", " + spearman._logMiner._numViews.get(s)+ ", "+spearman._corpusAnalyzer.getRank(s));
                //System.out.println(s + ", " + spearman.pageRank.get(s));
                System.out.println(s + ", " + spearman.numViews_Rank.get(s)+ ", "+spearman.pageRank_Rank.get(s));
                count++;
            }
            
            System.out.println("The spearman coefficient is: ");
            System.out.println(spearman.compute_Spearman(spearman.numViews_Rank, spearman.pageRank_Rank));
        }
    }
}
