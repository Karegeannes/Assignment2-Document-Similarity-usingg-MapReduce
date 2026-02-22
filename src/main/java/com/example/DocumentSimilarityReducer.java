package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String, Set<String>> docWordSets = new HashMap<>();
    private final Map<String, Integer> pairIntersection = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String word = key.toString();
        Set<String> docsForWord = new HashSet<>();

        for (Text val : values) {
            docsForWord.add(val.toString());
        }

        for (String docId : docsForWord) {
            docWordSets.computeIfAbsent(docId, k -> new HashSet<>()).add(word);
        }

        List<String> docList = new ArrayList<>(docsForWord);
        Collections.sort(docList);

        for (int i = 0; i < docList.size(); i++) {
            for (int j = i + 1; j < docList.size(); j++) {
                String pairKey = docList.get(i) + "," + docList.get(j);
                pairIntersection.merge(pairKey, 1, Integer::sum);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        List<String> pairs = new ArrayList<>(pairIntersection.keySet());
        Collections.sort(pairs);

        for (String pair : pairs) {
            String[] docs = pair.split(",");
            String docA = docs[0];
            String docB = docs[1];

            Set<String> setA = docWordSets.getOrDefault(docA, Collections.emptySet());
            Set<String> setB = docWordSets.getOrDefault(docB, Collections.emptySet());

            int intersection = pairIntersection.get(pair);
            int union = setA.size() + setB.size() - intersection;

            double jaccard = (union == 0) ? 0.0 : (double) intersection / union;

            context.write(
                new Text(docA + ", " + docB),
                new Text(String.format("%.4f", jaccard))
            );
        }
    }
}
