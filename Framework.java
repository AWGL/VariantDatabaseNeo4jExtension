package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonGenerator;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.util.Map;

/**
 * A class of functions for working with Neo4j DB
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
class Framework {
    static void writeNodeProperties(final Long id, final Map<String, Object> properties, final Iterable<Label> labels, final JsonGenerator jg) throws IOException {

        jg.writeNumberField("id", id);
        jg.writeObjectField("properties", properties);

        jg.writeArrayFieldStart("labels");
        for (Label label : labels){
            jg.writeString(label.name());
        }
        jg.writeEndArray();

    }
    static void writeRelationshipProperties(final Long id, final Map<String, Object> properties, final String name, final JsonGenerator jg) throws IOException {
        jg.writeNumberField("id", id);
        jg.writeObjectField("properties", properties);
        jg.writeStringField("type", name);
    }
    static Node matchOrCreateUniqueNode(GraphDatabaseService graphDb, Label label, String field, Object value) {
        Node node = null;

        //match
        try (Transaction tx = graphDb.beginTx()) {
            node = graphDb.findNode(label, field, value);
        }

        if (node != null) {
            return node;
        }

        //or create
        try (Transaction tx = graphDb.beginTx()) {
            node = graphDb.createNode(label);
            node.setProperty(field,value);

            tx.success();
        }

        return node;
    }
    static Node findDatasetNode(String sampleId, String worklistId, String seqId, GraphDatabaseService graphDb){

        try (Transaction tx = graphDb.beginTx()) {
            Node sampleNode = graphDb.findNode(Labels.sample, "sampleId", sampleId);
            for (Relationship hasDataRelationship : sampleNode.getRelationships(Direction.OUTGOING, Relationships.hasData)){
                Node node = hasDataRelationship.getEndNode();

                if (node.getProperty("worklistId").equals(worklistId) && node.getProperty("seqId").equals(seqId)){
                    return node;
                }

            }
        }

        throw new NullPointerException("Could not find data node");
    }

}
