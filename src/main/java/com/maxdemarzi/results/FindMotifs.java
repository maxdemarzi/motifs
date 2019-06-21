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
                "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m3_2 = new ArrayList<String>(){{
            add("m3_2");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p1) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p2 <> p3 " +
                "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m4_1 = new ArrayList<String>(){{
            add("m4_1");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4) " +
                "WHERE ID(r2) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m4_2 = new ArrayList<String>(){{
            add("m4_2");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3), (p2)-[r3]-(p4) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3), (p2)-[r3]-(p4) " +
                "WHERE ID(r3) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m4_3 = new ArrayList<String>(){{
            add("m4_3");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p2) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p2) " +
                "WHERE ID(r2) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p2) " +
                "WHERE ID(r3) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m4_4 = new ArrayList<String>(){{
            add("m4_4");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m4_5 = new ArrayList<String>(){{
            add("m4_5");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4) " +
                "WHERE ID(r1) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4) " +
                "WHERE ID(r5) = $rel_id " +
                "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                "RETURN COUNT(*) AS count");

        }};

        ArrayList<String> m4_6 = new ArrayList<String>(){{
            add("m4_6");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4), (p1)-[r6]-(p3) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4), (p1)-[r6]-(p3) " +
                    "WHERE ID(r5) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p1), (p2)-[r5]-(p4), (p1)-[r6]-(p3) " +
                    "WHERE ID(r6) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p2 <> p3 AND p2 <> p4 AND p3 <> p4 " +
                    "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m5_1 = new ArrayList<String>(){{
            add("m5_1");
            add("MATCH (p1)-[r1]-(p3)-[r2]-(p5)-[r3]-(p1), (p2)-[r4]-(p3)-[r5]-(p4)-[r6]-(p5)-[r7]-(p2) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p3)-[r2]-(p5)-[r3]-(p1), (p2)-[r4]-(p3)-[r5]-(p4)-[r6]-(p5)-[r7]-(p2) " +
                    "WHERE ID(r2) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p3)-[r2]-(p5)-[r3]-(p1), (p2)-[r4]-(p3)-[r5]-(p4)-[r6]-(p5)-[r7]-(p2) " +
                    "WHERE ID(r4) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m5_2 = new ArrayList<String>(){{
            add("m5_2");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                    "WHERE ID(r2) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                    "WHERE ID(r3) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                    "WHERE ID(r4) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                    "WHERE ID(r5) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p2), (p5)-[r6]-(p1) " +
                    "WHERE ID(r6) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m5_3 = new ArrayList<String>(){{
            add("m5_3");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                    "WHERE ID(r2) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                    "WHERE ID(r3) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                    "WHERE ID(r4) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                    "WHERE ID(r5) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4), (p3)-[r4]-(p5)-[r5]-(p1)-[r6]-(p3) " +
                    "WHERE ID(r6) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m5_4 = new ArrayList<String>(){{
            add("m5_4");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1)-[r6]-(p4)-[r7]-(p2) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1)-[r6]-(p4)-[r7]-(p2) " +
                    "WHERE ID(r2) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p1)-[r6]-(p4)-[r7]-(p2) " +
                    "WHERE ID(r3) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m5_5 = new ArrayList<String>(){{
            add("m5_5");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5), (p3)-[r5]-(p5)-[r6]-(p1)-[r7]-(p3) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5), (p3)-[r5]-(p5)-[r6]-(p1)-[r7]-(p3) " +
                    "WHERE ID(r2) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5), (p3)-[r5]-(p5)-[r6]-(p1)-[r7]-(p3) " +
                    "WHERE ID(r5) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5), (p3)-[r5]-(p5)-[r6]-(p1)-[r7]-(p3) " +
                    "WHERE ID(r6) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");
        }};

        ArrayList<String> m5_6 = new ArrayList<String>(){{
            add("m5_6");
            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p2), (p3)-[r6]-(p5)-[r7]-(p1)-[r8]-(p3) " +
                    "WHERE ID(r1) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p2), (p3)-[r6]-(p5)-[r7]-(p1)-[r8]-(p3) " +
                    "WHERE ID(r2) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p2), (p3)-[r6]-(p5)-[r7]-(p1)-[r8]-(p3) " +
                    "WHERE ID(r3) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");

            add("MATCH (p1)-[r1]-(p2)-[r2]-(p3)-[r3]-(p4)-[r4]-(p5)-[r5]-(p2), (p3)-[r6]-(p5)-[r7]-(p1)-[r8]-(p3) " +
                    "WHERE ID(r6) = $rel_id " +
                    "AND p1 <> p2 AND p1 <> p3 AND p1 <> p4 AND p1 <> p5 AND p2 <> p3 AND p2 <> p4 AND p2 <> p5 AND p3 <> p4 AND p3 <> p5 AND p4 <> p5 " +
                    "RETURN COUNT(*) AS count");
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

                while (relIds.hasNext()) {
                    long relId = relIds.next();
                    Relationship relationship = db.getRelationshipById(relId);
                    csvAppender.appendField(String.valueOf(relationship.getStartNodeId()));
                    csvAppender.appendField(String.valueOf(relationship.getEndNodeId()));

                    parameters.put("rel_id", relId);

                    for (ArrayList<String> pattern : patterns) {
                        Long count = 0L;
                        for (String motif : pattern.subList(1, pattern.size())) {
                            count += (Long)db.execute(motif, parameters).next().get("count");
                        }

                        csvAppender.appendField(String.valueOf(count));
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
