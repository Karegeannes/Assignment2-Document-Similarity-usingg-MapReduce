package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text docName = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        //Use first token as document identifier
        String docId = itr.nextToken();
        docName.set(docId);
        //Use remaining tokens as words
        while (itr.hasMoreTokens()) {
            String currentWord = itr.nextToken().toLowerCase();
            if (currentWord.length() >= 3) {
                word.set(currentWord);
                context.write(word, docName);
            }
        }
    }
}