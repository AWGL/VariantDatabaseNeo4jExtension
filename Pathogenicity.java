package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * A class for working with variants
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/pathogenicity")
public class Pathogenicity {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Pathogenicity(@Context GraphDatabaseService graphDb, @Context Log log){
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * POST {variantId:"variantId", userId:"userId", classification:classification, evidence:"evidence"} to /variantdatabase/add/pathogenicity
     * Returns occurrence of a variant
     */
    @POST
    @Path("/pathogenicity/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addPathogenicity(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);
            int classification = jsonNode.get("classification").asInt();

            //check classification is in range
            if (classification < 1 || classification > 5){
                throw new IllegalArgumentException("Illegal classification. Accepted values are one to five inclusive");
            }

            //get nodes
            Node variantNode = graphDb.findNode(Labels.variant, "variantId", jsonNode.get("variantId"));
            Node userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email"));

            //check variant does not already have outstanding auths
            Node lastEventNode = Event.getLastUserEventNode(variantNode, graphDb);

            if (lastEventNode.getId() != variantNode.getId()){
                Event.UserEventStatus status = Event.getUserEventStatus(lastEventNode, graphDb);

                if (status == Event.UserEventStatus.PENDING_AUTH){
                    throw new IllegalArgumentException("Cannot add pathogenicity. Auth pending.");
                }
            }

            //add properties
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("classification", classification);

            if (jsonNode.has("evidence")) {
                String evidence = jsonNode.get("evidence").asText();

                if (!evidence.equals("")){
                    properties.put("evidence", evidence);
                }
            }

            //add event
            Event.addUserEvent(lastEventNode, Labels.pathogenicity, properties, userNode, graphDb);

            return Response
                    .status(Response.Status.OK)
                    .build();

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

    /**
     * GET /variantdatabase/pathogenicity/auth/pending
     * Returns list of variant classifications awaiting authorisation
     */
    @GET
    @Path("/pathogenicity/auth/pending")
    @Produces(MediaType.APPLICATION_JSON)
    public Response authPending() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> pathogenicityIterator = graphDb.findNodes(Labels.pathogenicity)){

                            while (pathogenicityIterator.hasNext()) {
                                Node pathogenicity = pathogenicityIterator.next();

                                if (Event.getUserEventStatus(pathogenicity, graphDb) == Event.UserEventStatus.PENDING_AUTH){

                                    Relationship addedByRelationship = pathogenicity.getSingleRelationship(Relationships.addedBy, Direction.OUTGOING);

                                    jg.writeStartObject();

                                    jg.writeObjectFieldStart("pathogenicity");
                                    Framework.writeNodeProperties(pathogenicity.getId(), pathogenicity.getAllProperties(), pathogenicity.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("addedBy");
                                    User.writeLiteUserRecord(addedByRelationship.getEndNode(), jg, graphDb);
                                    jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                                    jg.writeEndObject();

                                    Node variantNode = Event.getSubjectNodeFromEventNode(pathogenicity, graphDb);

                                    jg.writeObjectFieldStart("variant");
                                    Framework.writeNodeProperties(variantNode.getId(), variantNode.getAllProperties(), variantNode.getLabels(), jg);
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

}
