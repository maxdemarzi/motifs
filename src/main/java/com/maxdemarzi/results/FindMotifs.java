package com.maxdemarzi.results;

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class FindMotifs implements Runnable {
    private final GraphDatabaseService db;
    private final Log log;
    private final Roaring64NavigableMap rels;
    private final String path;

    private static final ArrayList<ArrayList<String>> patterns;

    public FindMotifs(GraphDatabaseService db, Log log, Roaring64NavigableMap rels, int thread, String path) {
      this.db = db;
      this.log = log;
      this.rels = rels;
      this.path = path.substring(0, path.lastIndexOf("."))
              + "-" + thread
              + path.substring(path.lastIndexOf("."));
    }

    static  {
        patterns = new ArrayList<>();
        ArrayList<String> m3_1 = new ArrayList<String>(){{
            add("m3_1");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p2 <> p3 " +
                "RETURN [ID(r1), ID(r2)] AS relationships");
        }};

        ArrayList<String> m3_2 = new ArrayList<String>(){{
            add("m3_2");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p1) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p2 <> p3 " +
                "RETURN [ID(r1), ID(r2), ID(r3)] AS relationships");
        }};

        ArrayList<String> m4_1 = new ArrayList<String>(){{
            add("m4_1");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN [ID(r1), ID(r2), ID(r3)] AS relationships");
        }};

        ArrayList<String> m4_2 = new ArrayList<String>(){{
            add("m4_2");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3), (p2)-[r3]-(p4) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN [ID(r1), ID(r2), ID(r3)] AS relationships");
        }};

        ArrayList<String> m4_3 = new ArrayList<String>(){{
            add("m4_3");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4)] AS relationships");
        }};

        ArrayList<String> m4_4 = new ArrayList<String>(){{
            add("m4_4");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4)] AS relationships");
        }};

        ArrayList<String> m4_5 = new ArrayList<String>(){{
            add("m4_5");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r5) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5)] AS relationships");
        }};

        ArrayList<String> m4_6 = new ArrayList<String>(){{
            add("m4_6");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4), (p1)-[r6]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r5) = $rel_id OR ID(r6) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6)] AS relationships");
        }};

        ArrayList<String> m5_1 = new ArrayList<String>(){{
            add("m5_1");
            add("MATCH (p1)-[r1]-(p3)-[r2]-(p5)-[r3]-(p1), (p2)-[r4]-(p3)-[r5]-(p4)-[r6]-(p5)-[r7]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r4) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7)] AS relationships");
        }};

        ArrayList<String> m5_2 = new ArrayList<String>(){{
            add("m5_2");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r4) = $rel_id OR ID(r5) = $rel_id OR ID(r6) = $rel_id) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6)] AS relationships");
        }};

        ArrayList<String> m5_3 = new ArrayList<String>(){{
            add("m5_3");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r4) = $rel_id OR ID(r5) = $rel_id OR ID(r6) = $rel_id) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6)] AS relationships");
        }};

        ArrayList<String> m5_4 = new ArrayList<String>(){{
            add("m5_4");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1)-[r6]-(p4)-[r7]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7)] AS relationships");
        }};

        ArrayList<String> m5_5 = new ArrayList<String>(){{
            add("m5_5");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5), (p3)-[r5]-(p5)-[r6]-(p1)-[r7]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r5) = $rel_id OR ID(r6) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7)] AS relationships");
        }};

        ArrayList<String> m5_6 = new ArrayList<String>(){{
            add("m5_6");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p2), (p3)-[r6]-(p5)-[r7]-(p1)-[r8]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r6) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7), ID(r8)] AS relationships");
        }};

        ArrayList<String> m5_7 = new ArrayList<String>(){{
            add("m5_7");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1)-[r7]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7)] AS relationships");
        }};

        ArrayList<String> m5_8 = new ArrayList<String>(){{
            add("m5_8");
            add("MATCH (p1)-[r1]-(p3)-[r2]-(p4)-[r3]-(p5)-[r4]-(p1)-[r5]-(p4)-[r6]-(p2)-[r7]-(p5)-[r8]-(p3)-[r9]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r5) = $rel_id OR ID(r6) = $rel_id OR ID(r7) = $rel_id OR ID(r8) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7), ID(r8), ID(r9)] AS relationships");
        }};

        ArrayList<String> m5_9 = new ArrayList<String>(){{
            add("m5_9");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r4) = $rel_id OR ID(r5) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5)] AS relationships");
        }};

        ArrayList<String> m5_10 = new ArrayList<String>(){{
            add("m5_10");
            add("MATCH (p1)-[r1]-(p5), (p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r3) = $rel_id OR ID(r5) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5)] AS relationships");
        }};

        ArrayList<String> m5_11 = new ArrayList<String>(){{
            add("m5_11");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p4)-[r3]-(p5)-[r4]-(p3)-[r5]-(p4)-[r6]-(p1) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6)] AS relationships");
        }};

        ArrayList<String> m5_12 = new ArrayList<String>(){{
            add("m5_12");
            add("MATCH (p1)-[r1]-(p4)-[r2]-(p5)-[r3]-(p3)-[r4]-(p4)-[r5]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5)] AS relationships");
        }};

        ArrayList<String> m5_13 = new ArrayList<String>(){{
            add("m5_13");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1), (p5)-[r6]-(p3) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r6) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6)] AS relationships");
        }};

        ArrayList<String> m5_14 = new ArrayList<String>(){{
            add("m5_14");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p5)-[r4]-(p3)-[r5]-(p1)-[r6]-(p5)-[r7]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7)] AS relationships");
        }};

        ArrayList<String> m5_15 = new ArrayList<String>(){{
            add("m5_15");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5)] AS relationships");
        }};

        ArrayList<String> m5_16 = new ArrayList<String>(){{
            add("m5_16");
            add("MATCH (p1)-[r1]-(p3)-[r2]-(p4)-[r3]-(p5)-[r4]-(p1), (p5)-[r5]-(p2)-[r6]-(p3) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6)] AS relationships");
        }};

        ArrayList<String> m5_17 = new ArrayList<String>(){{
            add("m5_17");
            add("MATCH (p1)-[r1]-(p5)-[r2]-(p4)-[r3]-(p3)-[r4]-(p2) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r4) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4)] AS relationships");
        }};

        ArrayList<String> m5_18 = new ArrayList<String>(){{
            add("m5_18");
            add("MATCH (p1)-[r1]-(p4)-[r2]-(p2), (p5)-[r3]-(p4)-[r4]-(p3) " +
                "WHERE ID(r1) = $rel_id  " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4)] AS relationships");
        }};

        ArrayList<String> m5_19 = new ArrayList<String>(){{
            add("m5_19");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p4)-[r3]-(p3), (p4)-[r4]-(p5) " +
                "WHERE ( ID(r1) = $rel_id OR ID(r2) = $rel_id OR ID(r3) = $rel_id OR ID(r4) = $rel_id ) " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4)] AS relationships");
        }};

        ArrayList<String> m5_20 = new ArrayList<String>(){{
            add("m5_20");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5)] AS relationships");
        }};

        ArrayList<String> m5_21 = new ArrayList<String>(){{
            add("m5_21");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3)-[r7]-(p5)-[r8]-(p2)-[r9]-(p4)-[r10]-(p1) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                "RETURN [ID(r1), ID(r2), ID(r3), ID(r4), ID(r5), ID(r6), ID(r7), ID(r8), ID(r9), ID(r10)] AS relationships");
        }};

        patterns.add(m3_1);
        patterns.add(m3_2);
        patterns.add(m4_1);
        patterns.add(m4_2);
        patterns.add(m4_3);
        patterns.add(m4_4);
        patterns.add(m4_5);
        patterns.add(m4_6);
        patterns.add(m5_1);
        patterns.add(m5_2);
        patterns.add(m5_3);
        patterns.add(m5_4);
        patterns.add(m5_5);
        patterns.add(m5_6);
        patterns.add(m5_7);
        patterns.add(m5_8);
        patterns.add(m5_9);
        patterns.add(m5_10);
        patterns.add(m5_11);
        patterns.add(m5_12);
        patterns.add(m5_13);
        patterns.add(m5_14);
        patterns.add(m5_15);
        patterns.add(m5_16);
        patterns.add(m5_17);
        patterns.add(m5_18);
        patterns.add(m5_19);
        patterns.add(m5_20);
        patterns.add(m5_21);
    }

    @Override
    public void run() {
        File file = new File(path);
        CsvWriter csvWriter = new CsvWriter();
        try (CsvAppender csvAppender = csvWriter.append(file, StandardCharsets.UTF_8)) {
            csvAppender.appendField("from");
            csvAppender.appendField("to");
            for (ArrayList<String> pattern : patterns) {
                csvAppender.appendField(pattern.get(0));
            }
            csvAppender.endLine();

            Iterator<Long> relIds = rels.iterator();

            try(Transaction tx = db.beginTx()) {
                Map<String, Object> parameters = new HashMap<>();
                Result result;
                List<Long> relationships;

                while (relIds.hasNext()) {
                    long relId = relIds.next();
                    Relationship relationship = db.getRelationshipById(relId);
                    csvAppender.appendField(String.valueOf(relationship.getStartNodeId()));
                    csvAppender.appendField(String.valueOf(relationship.getEndNodeId()));

                    parameters.put("rel_id", relId);

                    for (ArrayList<String> pattern : patterns) {
                        Set<List<Long>> allRelationships = new HashSet<>();
                        for (String motif : pattern.subList(1, pattern.size())) {

                            result = db.execute(motif, parameters);

                            while (result.hasNext()) {
                                boolean unique = true;
                                relationships = (List<Long>)result.next().get("relationships");

                                for (List<Long> rels : allRelationships) {
                                    if (rels.containsAll(relationships)) {
                                        unique = false;
                                        break; };
                                }
                                if (unique) {
                                    allRelationships.add(relationships);
                                }
                            }
                            csvAppender.appendField(String.valueOf(allRelationships.size()));
                        }
                    }

                    csvAppender.endLine();
                }


                tx.success();
            }

        } catch (IOException exception) {
            log.error("An error occurred in FindMotifs: ");
            log.error(Arrays.stream(exception.getStackTrace())
                    .map(Objects::toString)
                    .collect(Collectors.joining("\n")));
        }
    }
}
