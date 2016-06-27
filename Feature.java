package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.server.rest.web.NodeNotFoundException;

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
import java.util.HashMap;

/**
 * A class for working with features
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/feature")
public class Feature {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Feature(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * Returns info about a feature
     * @param json {featureId}
     */
    @POST
    @Path("/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response info(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartObject();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node node = graphDb.findNode(Labels.feature, "featureId", jsonNode.get("featureId").asText());

                        Framework.writeNodeProperties(node.getId(), node.getAllProperties(), node.getLabels(), jg);
                        Event.writeEventHistory(node, jg, graphDb);
                    }

                    jg.writeEndObject();

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
     * Adds preference to feature
     * @param json {featureId, email, preference, evidence}
     */
    @POST
    @Path("preference/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response preferenceAdd(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);
            Node featureNode, userNode;

            //get nodes
            try (Transaction tx = graphDb.beginTx()) {
                featureNode = graphDb.findNode(Labels.feature, "featureId", jsonNode.get("featureId").asText());
                userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email").asText());
            }

            if (featureNode == null){
                throw new NotFoundException("Could not find feature");
            }
            if (userNode == null){
                throw new NotFoundException("Could not find user");
            }

            //check subject not already have outstanding auths
            Node lastEventNode = Event.getLastUserEventNode(featureNode, graphDb);

            if (lastEventNode.getId() != featureNode.getId()){
                Event.UserEventStatus status = Event.getUserEventStatus(lastEventNode, graphDb);

                if (status == Event.UserEventStatus.PENDING_AUTH){
                    throw new IllegalArgumentException("Cannot add preference. Auth pending.");
                }
            }

            //add event
            try (Transaction tx = graphDb.beginTx()) {
                Node newEventNode = graphDb.createNode(Labels.featurePreference);

                //add properties
                newEventNode.setProperty("preference", jsonNode.get("preference").asBoolean());

                if (jsonNode.has("evidence") && !jsonNode.get("evidence").asText().equals("")) {
                    newEventNode.setProperty("evidence", jsonNode.get("evidence").asText());
                }

                Relationship addedByRelationship = newEventNode.createRelationshipTo(userNode, Relationships.addedBy);
                addedByRelationship.setProperty("date", new Date().getTime());

                lastEventNode.createRelationshipTo(newEventNode, Relationships.hasEvent);

                tx.success();
            }

            return Response.status(Response.Status.OK).build();

        } catch (IllegalArgumentException e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.BAD_REQUEST)
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


}
