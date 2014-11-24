package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

public class Spearman2{
	private Map<String, Double> numViews = new HashMap<String, Double>();
	private Map<String, Double> pageRank = new HashMap<String, Double>();
	String numViews_path = "data/numview.idx";
	String pageRank_path = "data/pagerank.idx";
	private Map<String, Integer> numViews_Rank = new HashMap<String, Integer>();
	private Map<String, Integer> pageRank_Rank = new HashMap<String, Integer>();
	
	
	public Map<String, Double> load(String file_path) throws IOException {
	    System.out.println("Loading from " + file_path);
	    BufferedReader br_Reader = new BufferedReader(new FileReader(file_path));
	    Map<String, Double> original_map = new HashMap<String, Double>();
	    String line = br_Reader.readLine();
	    String[] tmp;
	    while(line != null){
	    	tmp = line.trim().split(" ");
	    	original_map.put(tmp[0], Double.parseDouble(tmp[1]));
	    	line = br_Reader.readLine();
	    }
	    br_Reader.close();
	    return original_map;
	  }
	
	
	public Map<String, Integer> sort_map(Map<String, Double> original_map){
		Map<String, Integer> sort_map = new HashMap<String, Integer>();
		
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(original_map.entrySet());
		Collections.sort(list,new Comparator<Map.Entry<String,Double>>(){
			public int compare(Map.Entry<String, Double> doc1,
					Map.Entry<String, Double> doc2) {
				if(doc2.getValue() == doc1.getValue()) {
					return doc1.getKey().compareTo(doc2.getKey());
				}
				return (doc2.getValue() - doc1.getValue() > 0)? 1 : 0;
			}	
		});
		int rank = 1;
		for(Map.Entry<String, Double> doc : list) {
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
	
}