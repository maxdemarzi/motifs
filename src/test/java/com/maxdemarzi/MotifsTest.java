package com.maxdemarzi;

import org.junit.jupiter.api.*;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class MotifsTest {
    private static ServerControls neo4j;

    @BeforeAll
    static void startNeo4j() {
        neo4j = TestServerBuilders.newInProcessBuilder()
                .withProcedure(Procedures.class)
                .withFixture(MODEL_STATEMENT)
                .newServer();
    }

    @AfterAll
    static void stopNeo4j() {
        neo4j.close();
    }

    @Test
    void shouldFindMotifs()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.motifs($type, $file)",
                    parameters( "type", "ALSO_PURCHASED", "file", "/tmp/also_purchased_motifs.csv" ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asString()).startsWith("Results written to");
        }
    }

    private static final String MODEL_STATEMENT =
            "CREATE (p1:Product)" +
            "CREATE (p2:Product)" +
            "CREATE (p3:Product)" +
            "CREATE (p4:Product)" +
            "CREATE (p5:Product)" +
            "CREATE (p6:Product)" +
            "CREATE (p7:Product)" +
            "CREATE (p1)-[:ALSO_PURCHASED]->(p2)" +
            "CREATE (p2)-[:ALSO_PURCHASED]->(p3)" +
            "CREATE (p3)-[:ALSO_PURCHASED]->(p4)" +
            "CREATE (p4)-[:ALSO_PURCHASED]->(p5)" +
            "CREATE (p5)-[:ALSO_PURCHASED]->(p1)" +
            "CREATE (p1)-[:ALSO_PURCHASED]->(p3)" +
            "CREATE (p3)-[:ALSO_PURCHASED]->(p5)" +
            "CREATE (p5)-[:ALSO_PURCHASED]->(p2)" +
            "CREATE (p2)-[:ALSO_PURCHASED]->(p4)" +
            "CREATE (p4)-[:ALSO_PURCHASED]->(p1)" +
            "CREATE (p1)-[:ALSO_PURCHASED]->(p6)" +
            "CREATE (p2)-[:ALSO_PURCHASED]->(p7)" +
            "CREATE (p6)-[:ALSO_PURCHASED]->(p7)";
}
