package com.maxdemarzi;

import com.maxdemarzi.results.FindMotifs;
import com.maxdemarzi.results.StringResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Procedures {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/neo4j.log`
    @Context
    public Log log;


    @Procedure(name = "com.maxdemarzi.motifs", mode = Mode.WRITE)
    @Description("CALL com.maxdemarzi.motifs(type, path)")
    public Stream<StringResult> motifs(@Name("type") String type, @Name(value = "path", defaultValue = "/tmp/motifs.csv") String path) throws InterruptedException {
        // Get number of threads and setup executor
        int threads = Runtime.getRuntime().availableProcessors();
        final ExecutorService service = Executors.newFixedThreadPool(threads);

        // Find all the relationships of the specified type
        RelationshipType relationshipType = RelationshipType.withName(type);
        Roaring64NavigableMap[] rels = new Roaring64NavigableMap[threads];
        for(int i = 0; i < threads; i++) {
            rels[i] = new Roaring64NavigableMap();
        }

        AtomicInteger index = new AtomicInteger(-1);

        // Get relationship IDs and split them into buckets
        for (Relationship r : db.getAllRelationships()) {
            if (r.isType(relationshipType)) {
                rels[index.incrementAndGet() % threads].add(r.getId());
            }
        }

        for (int i = 0; i < threads; i++) {
            service.execute(new FindMotifs(db, log, rels[i], i, path));
        }

        try {
            service.shutdown();
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("tasks interrupted");
        } finally {
            if (!service.isTerminated()) {
                log.error("cancel tasks");
            }
            service.shutdownNow();
            log.info("shutdown finished");
        }

        return Stream.of(new StringResult("Results written to: " + path + "1-" + threads));
    }
}
