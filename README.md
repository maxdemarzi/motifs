# Motifs

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/motifs-1.0-SNAPSHOT.jar neo4j-enterprise-3.5.6/plugins/.
    

Restart your Neo4j Server. Your new Stored Procedures are available:

    CALL com.maxdemarzi.motifs("relationshipType", "filepath");    
    CALL com.maxdemarzi.motifs("ALSO_PURCHASED", "/tmp/also_purchased_motifs.csv");


##### Sample Data

    CREATE (p1:Product)
    CREATE (p2:Product)
    CREATE (p3:Product)
    CREATE (p4:Product)
    CREATE (p5:Product)
    CREATE (p1)-[:ALSO_PURCHASED]->(p2)
    CREATE (p2)-[:ALSO_PURCHASED]->(p3)
    CREATE (p3)-[:ALSO_PURCHASED]->(p4)
    CREATE (p4)-[:ALSO_PURCHASED]->(p5)
    CREATE (p5)-[:ALSO_PURCHASED]->(p1)
    CREATE (p1)-[:ALSO_PURCHASED]->(p3)
    CREATE (p3)-[:ALSO_PURCHASED]->(p5)
    CREATE (p5)-[:ALSO_PURCHASED]->(p2)
    CREATE (p2)-[:ALSO_PURCHASED]->(p4)
    CREATE (p4)-[:ALSO_PURCHASED]->(p1);