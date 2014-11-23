package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Vector;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Handles each incoming query, students do not need to change this class except
 * to provide more query time CGI arguments and the HTML output.
 *
 * N.B. This class is not thread-safe.
 *
 * @author congyu
 * @author fdiaz
 */
class PseudoRev
{

    // For accessing the underlying documents to be used by the Ranker. Since 
    // we are not worried about thread-safety here, the Indexer class must take
    // care of thread-safety.
    private Options options;
    private BufferedReader docTermFreqReader;
    
    public PseudoRev(Options options)
    {
        this.options = options;
    }

    public List<WPPair> computePRF(Vector<ScoredDocument> scoredDocs, int K, int m)
    {
        //Map<WDPair, Integer> docTermFreq = new HashMap<WDPair, Integer>();
        Map<String, Integer> termFreq = new HashMap<String, Integer>();
        Map<String, Double> termProbs = new HashMap<String, Double>();
        List<WPPair> probListFinal = new ArrayList<WPPair>();
        
        int totalWordCount = 0;
        ArrayList<Integer> topK = new ArrayList<Integer>();
        
        for(int i=0;i<K;i++)
            topK.add(scoredDocs.get(i).getDocument()._docid);
        
        if(topK.size() == 0)
            return probListFinal;
        Collections.sort(topK);
        
        //get stuff from files
        try
        {
            docTermFreqReader = new BufferedReader(new FileReader(new File(options._indexPrefix + "/dft_list")));
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        String line = "";
        
        Iterator<Integer> iter = topK.iterator();
        int toRead = iter.next();
        int lineNum = 0;
        try
        {
            while((line = docTermFreqReader.readLine()) != null)
            {
                lineNum++;
                if(lineNum == toRead)
                {
                    System.out.println("Doc = " + lineNum);
                    try
                    {
                        String strs[] = line.split(" ");

                        //for(String s : strs)
                        //int min2 = (strs.length < 2*m) ? strs.length : 2*m;
                        //for(int j=0;j<min2;j+=2)
                        for(int j=0;j<strs.length;j+=2)
                        {
                            //System.out.println("word = " + strs[j] + ", count = " + strs[j+1]);
                            /*
                            WDPair wd = new WDPair(strs[j], Integer.parseInt(strs[j+1]));
                            if(docTermFreq.containsKey(wd))
                                docTermFreq.put(wd, docTermFreq.get(wd)+1);
                            else
                                docTermFreq.put(wd, 1);
                            */
                            int c = Integer.parseInt(strs[j+1]);
                            totalWordCount += c;
                            if(termFreq.containsKey(strs[j]))
                                termFreq.put(strs[j], termFreq.get(strs[j]) + c);
                            else
                                termFreq.put(strs[j], c);
                                    
                        }
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }

                    if (iter.hasNext())
                        toRead = iter.next();
                    else
                    {
                        //?
                    }
                }

            }
            
            for(Entry<String,Integer> entry : termFreq.entrySet())
            {
                double prob = (double)entry.getValue() / totalWordCount;
                termProbs.put(entry.getKey(), prob);
            }
            
            //termProbs
            List probList = new ArrayList(termProbs.entrySet());
            
            Collections.sort(probList, new Comparator() {
                public int compare(Object o1, Object o2) 
                {
                   return -1*((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
                }
            });
            
            int min3 = (probList.size() < m) ? probList.size() : m;
            double sum = 0;
            for(int i=0;i<min3;i++)
            {
                Entry elem = (Entry)probList.get(i);
                //System.out.println("top word = " + elem.getKey() + ", prob = " + elem.getValue());
                sum += (Double)elem.getValue();
                
            }
            //System.out.println("Sum = " + sum);
            for(int i=0;i<min3;i++)
            {
                Entry elem = (Entry)probList.get(i);
                double normalizedProb = (Double)elem.getValue() / sum;
                //elem.setValue(normalizedProb);
                
                probListFinal.add(new WPPair((String)elem.getKey(), normalizedProb));
                //System.out.println("top word = " + elem.getKey() + ", normalized prob = " + normalizedProb);
                
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            return probListFinal;
        }
        
        
    }
    
    class WPPair
    {
        String term;
        double prob;

        public WPPair(String term, double prob)
        {
            this.term = term;
            this.prob = prob;
        }

        
    }
}
