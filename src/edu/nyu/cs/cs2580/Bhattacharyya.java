/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 *
 * @author Ali Local
 */
public class Bhattacharyya
{
    private static HashMap<String, Double> getTermProbList(String filename)
    {
        HashMap<String, Double> map = new HashMap<String, Double>();
        try
        {
            BufferedReader bf = new BufferedReader(new FileReader(new File(filename)));
            String line = "";
            while((line = bf.readLine()) != null)
            {
                String strs[] = line.split("\\s");
                //System.out.println(strs[0]);
                //System.out.println(Double.parseDouble(strs[1]));
                map.put( strs[0], Double.parseDouble(strs[1]));
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return map;
        
        
    }
    public static double computeCoeff(String q1File, String q2File)
    {
        //V = union of words in query1's expansion and query2's expansion
        HashMap<String, Double> map1 = getTermProbList(q1File);
        HashMap<String, Double> map2 = getTermProbList(q2File);
        
        ArrayList<String> q2Exp = new ArrayList<String>((getTermProbList(q1File)).keySet());
        ArrayList<String> V = new ArrayList<String>((getTermProbList(q1File)).keySet());
        for(String s : q2Exp)
        {
            if(!V.contains(s))
                V.add(s);
        }
        
        double sum = 0;
        for(String w : V)
        {
            if(map1.containsKey(w) && map2.containsKey(w))
                sum  += Math.sqrt(map1.get(w) * map2.get(w));
        }
        return sum;
    }
    public static void main(String args[])
    {
        //double c = computeCoeff("", "", "res_scienc", "res_librari");
        
        //double c = computeCoeff("", "", "res_scienc", "res_scienc");
        //System.out.println("c = " + c);
        //for(int i=0;i<args.length;i++)
        //    System.out.println("args " + i + " => " + args[i]);
        if(args.length != 2)
        {
            System.out.println("Usage: Bhattacharyya <input-file> <output-file>");
            return;
        }
        
        String input = args[0];
        String output = args[1];
        HashMap<String,String> queryFileMap = new LinkedHashMap<String,String>();
        try
        {
            BufferedReader bf = new BufferedReader(new FileReader(new File(input)));
            String line = "";
            while((line = bf.readLine()) != null)
            {
                String strs[] = line.split(":");
                queryFileMap.put(strs[0],strs[1]);
            }
            
            
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output)));
            
            int i=0;
            for(Entry<String,String> entry : queryFileMap.entrySet())
            {
                i++;
                int j=0;
                for(Entry<String,String> entry2 : queryFileMap.entrySet())
                {
                    j++;
                    if(i==j) continue;

                    //System.out.print(entry.getKey() + "\t");
                    bw.write(entry.getKey() + "\t");
                    //System.out.print(entry2.getKey() + "\t");
                    bw.write(entry2.getKey() + "\t");

                    double c = computeCoeff(entry.getValue(), entry2.getValue());
                    //System.out.println(c);
                    bw.write(c + "\n");
                    
                }
                
            }
            bw.close();
            
        }
        catch(FileNotFoundException fne)
        {
            System.out.println("Input file not found");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
                
        
    }
}
