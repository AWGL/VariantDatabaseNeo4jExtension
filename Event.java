package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;

import javax.security.auth.login.CredentialException;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Date;

/**
 * A class for working with user events
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */

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

    /**
     * @return Returns all event nodes requiring auth
     * @param json {label}
     */
    @POST
    @Path("pending/auth")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response eventPendingAuth(final String json) {

        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> iter = graphDb.findNodes(Label.label(jsonNode.get("label").asText()))){

                            while (iter.hasNext()) {
                                Node eventNode = iter.next();

                                if (Event.getUserEventStatus(eventNode, graphDb) == Event.UserEventStatus.PENDING_AUTH){

                                    Relationship addedByRelationship = eventNode.getSingleRelationship(Relationships.addedBy, Direction.OUTGOING);

                                    jg.writeStartObject();

                                    Node subjectNode = Event.getSubjectNodeFromEventNode(eventNode, graphDb);
                                    jg.writeObjectFieldStart("subject");
                                    Framework.writeNodeProperties(subjectNode.getId(), subjectNode.getAllProperties(), subjectNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("event");
                                    Framework.writeNodeProperties(eventNode.getId(), eventNode.getAllProperties(), eventNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("addedBy");
                                    Node addedByUserNode = addedByRelationship.getEndNode();
                                    User.writeLiteUserRecord(addedByUserNode.getId(), addedByUserNode.getLabels(), (String) addedByUserNode.getProperty("email"), (String) addedByUserNode.getProperty("fullName"), (boolean) addedByUserNode.getProperty("admin"), jg);
                                    jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                                    jg.writeEndObject();

                                    jg.writeEndObject();

                                }

                            }

                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();

                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    /**
     * Auth user event
     * @param json {eventNodeId, email, addOrRemove}
     * @return response code
     */
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
                    throw new CredentialException("Admin rights required for this operation."); //todo check
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

    static void writeEventHistory(Node subjectNode, JsonGenerator jg, GraphDatabaseService graphDb) throws IOException {
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {

            //get longest path
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(Relationships.hasEvent, Direction.OUTGOING)
                    .traverse(subjectNode)) {
                longestPath = path;
            }

            jg.writeArrayFieldStart("history");

            //loop over nodes in this path
            for (Node eventNode : longestPath.nodes()) {
                if (eventNode.getId() == subjectNode.getId()) continue;

                jg.writeStartObject();

                Relationship addedByRelationship = eventNode.getSingleRelationship(Relationships.addedBy, Direction.OUTGOING);
                Relationship authorisedByRelationship = eventNode.getSingleRelationship(Relationships.authorisedBy, Direction.OUTGOING);
                Relationship rejectedByRelationship = eventNode.getSingleRelationship(Relationships.rejectedBy, Direction.OUTGOING);

                //event info
                Framework.writeNodeProperties(eventNode.getId(), eventNode.getAllProperties(), eventNode.getLabels(), jg);

                jg.writeObjectFieldStart("add");
                Node addedByNode = addedByRelationship.getEndNode();
                User.writeLiteUserRecord(addedByNode.getId(), addedByNode.getLabels(), (String) addedByNode.getProperty("fullName"), (String) addedByNode.getProperty("email"), (boolean) addedByNode.getProperty("admin"), jg);
                jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                jg.writeEndObject();

                if (authorisedByRelationship == null && rejectedByRelationship == null){
                    jg.writeStringField("status", UserEventStatus.PENDING_AUTH.toString());
                }

                if (authorisedByRelationship != null && rejectedByRelationship == null){
                    jg.writeStringField("status", UserEventStatus.ACTIVE.toString());

                    jg.writeObjectFieldStart("auth");
                    Node authorisedByNode = authorisedByRelationship.getEndNode();
                    User.writeLiteUserRecord(authorisedByNode.getId(), authorisedByNode.getLabels(), (String) authorisedByNode.getProperty("fullName"), (String) authorisedByNode.getProperty("email"), (boolean) authorisedByNode.getProperty("admin"), jg);
                    jg.writeNumberField("date",(long) authorisedByRelationship.getProperty("date"));
                    jg.writeEndObject();

                }

                if (authorisedByRelationship == null && rejectedByRelationship != null){
                    jg.writeStringField("status", UserEventStatus.REJECTED.toString());

                    jg.writeObjectFieldStart("auth");
                    Node rejectedByNode = rejectedByRelationship.getEndNode();
                    User.writeLiteUserRecord(rejectedByNode.getId(), rejectedByNode.getLabels(), (String) rejectedByNode.getProperty("fullName"), (String) rejectedByNode.getProperty("email"), (boolean) rejectedByNode.getProperty("admin"), jg);
                    jg.writeNumberField("date",(long) rejectedByRelationship.getProperty("date"));
                    jg.writeEndObject();

                }

                jg.writeEndObject();

            }

            jg.writeEndArray();
        }

    }

    private static Node getSubjectNodeFromEventNode(Node eventNode, GraphDatabaseService graphDb){

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
