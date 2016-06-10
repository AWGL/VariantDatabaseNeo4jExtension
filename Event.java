package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;

import javax.security.auth.login.CredentialException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * A class for working with user events
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */

//TODO
@Path("/variantdatabase/event")
public class Event {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    enum UserEventStatus {
        PENDING_AUTH, ACTIVE, REJECTED
    }

    public Event(@Context GraphDatabaseService graphDb, @Context Log log){
        this.graphDb = graphDb;
        this.log = log;
    }

    @POST
    @Path("/auth")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response auth(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);
            Node eventNode, userNode;

            try (Transaction tx = graphDb.beginTx()) {
                eventNode = graphDb.getNodeById(jsonNode.get("eventNodeId").asLong());
                userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email").asText());

                if (!(boolean) userNode.getProperty("admin")) {
                    throw new CredentialException("Admin rights required for this operation.");
                }
            }

            if (getUserEventStatus(eventNode, graphDb) != UserEventStatus.PENDING_AUTH) {
                throw new IllegalArgumentException("Event has no pending authorisation");
            }

            authUserEvent(eventNode, userNode, jsonNode.get("addOrRemove").asBoolean(), graphDb);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (CredentialException e){
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    static Node getSubjectNodeFromEventNode(Node eventNode, GraphDatabaseService graphDb){

        Node subjectNode = null;
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(Relationships.hasEvent, Direction.INCOMING)
                    .traverse(eventNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node node : longestPath.nodes()) {
                subjectNode = node;
            }

        }

        return subjectNode;
    }

    static Node getLastUserEventNode(Node subjectNode, GraphDatabaseService graphDb){

        Node lastEventNode = null;
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(Relationships.hasEvent, Direction.OUTGOING)
                    .traverse(subjectNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node node : longestPath.nodes()) {
                lastEventNode = node;
            }

        }

        return lastEventNode;
    }

    static Node getLastActiveUserEventNode(Node subjectNode, GraphDatabaseService graphDb){

        Node lastEventNode = null;
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(Relationships.hasEvent, Direction.OUTGOING)
                    .traverse(subjectNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node node : longestPath.nodes()) {
                if (getUserEventStatus(node, graphDb) == UserEventStatus.ACTIVE){
                    lastEventNode = node;
                }
            }

        }

        return lastEventNode;
    }

    static void addUserEvent(Node lastUserEventNode, Label newEventNodeLabel, HashMap<String, Object> properties, Node userNode, GraphDatabaseService graphDb) {

        try (Transaction tx = graphDb.beginTx()) {
            Node newEventNode = graphDb.createNode(newEventNodeLabel);

            for (Map.Entry<String, Object> iter : properties.entrySet()){
                newEventNode.setProperty(iter.getKey(), iter.getValue());
            }

            Relationship addedByRelationship = newEventNode.createRelationshipTo(userNode, Relationships.addedBy);
            addedByRelationship.setProperty("date", new Date().getTime());

            lastUserEventNode.createRelationshipTo(newEventNode, Relationships.hasEvent);

            tx.success();
        }

    }

    private static void authUserEvent(Node eventNode, Node userNode, boolean acceptOrReject, GraphDatabaseService graphDb){
        try (Transaction tx = graphDb.beginTx()) {
            Relationship authByRelationship = eventNode.createRelationshipTo(userNode, acceptOrReject ? Relationships.authorisedBy : Relationships.rejectedBy);
            authByRelationship.setProperty("date", new Date().getTime());
            tx.success();
        }
    }

    static UserEventStatus getUserEventStatus(Node eventNode, GraphDatabaseService graphDb) {
        Relationship addedbyRelationship, authorisedByRelationship, rejectedByRelationship;

        try (Transaction tx = graphDb.beginTx()) {
            addedbyRelationship = eventNode.getSingleRelationship(Relationships.addedBy, Direction.OUTGOING);
            authorisedByRelationship = eventNode.getSingleRelationship(Relationships.authorisedBy, Direction.OUTGOING);
            rejectedByRelationship = eventNode.getSingleRelationship(Relationships.rejectedBy, Direction.OUTGOING);
        }

        if (authorisedByRelationship == null && rejectedByRelationship == null){
            return UserEventStatus.PENDING_AUTH;
        }

        if (authorisedByRelationship != null && rejectedByRelationship == null){
            return UserEventStatus.ACTIVE;
        }

        if (authorisedByRelationship == null && rejectedByRelationship != null){
            return UserEventStatus.REJECTED;
        }

        return null;
    }
}
