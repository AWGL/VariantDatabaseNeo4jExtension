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
import java.util.Date;

/**
 * A class for working with datasets
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/dataset")
public class Dataset {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Dataset(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * Returns all QC passing datasets
     */
    @GET
    @Path("/info/passed")
    @Produces(MediaType.APPLICATION_JSON)
    public Response passed() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> datasetNodes = graphDb.findNodes(Labels.dataset)) {

                            while (datasetNodes.hasNext()) {
                                Node datasetNode = datasetNodes.next();

                                //check analysis has passed QC
                                Node lastEventNode = Event.getLastUserEventNode(datasetNode, graphDb);

                                if (lastEventNode.getId() != datasetNode.getId()){
                                    Event.UserEventStatus status = Event.getUserEventStatus(lastEventNode, graphDb);

                                    if (status == Event.UserEventStatus.ACTIVE && (boolean) lastEventNode.getProperty("passOrFail")){
                                        Node sampleNode = datasetNode.getSingleRelationship(Relationships.hasData, Direction.INCOMING).getStartNode();

                                        jg.writeStartObject();

                                        //write sample
                                        jg.writeObjectFieldStart("sample");
                                        Framework.writeNodeProperties(sampleNode.getId(), sampleNode.getAllProperties(), sampleNode.getLabels(), jg);
                                        jg.writeEndObject();

                                        //write dataset
                                        jg.writeArrayFieldStart("datasets");

                                        jg.writeStartObject();
                                        Framework.writeNodeProperties(datasetNode.getId(), datasetNode.getAllProperties(), datasetNode.getLabels(), jg);
                                        jg.writeEndObject();

                                        jg.writeEndObject();
                                    }

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
     * Returns all datasets requiring QC
     */
    @GET
    @Path("/info/pending")
    @Produces(MediaType.APPLICATION_JSON)
    public Response pending() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> iter = graphDb.findNodes(Labels.dataset)){

                            while (iter.hasNext()) {
                                Node datasetNode = iter.next();

                                //check run has passed QC
                                Node lastEventNode = Event.getLastUserEventNode(datasetNode, graphDb);

                                if (lastEventNode.getId() == datasetNode.getId()){
                                    jg.writeStartObject();

                                    Node sampleNode = datasetNode.getSingleRelationship(Relationships.hasData, Direction.INCOMING).getStartNode();
                                    jg.writeObjectFieldStart("sample");
                                    Framework.writeNodeProperties(sampleNode.getId(), sampleNode.getAllProperties(), sampleNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("datset");
                                    Framework.writeNodeProperties(datasetNode.getId(), datasetNode.getAllProperties(), datasetNode.getLabels(), jg);
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
     * Adds QC node to dataset
     * @param json {sampleId, worklistId, seqId, email, passOrFail, evidence}
     */
    @POST
    @Path("/add/qc")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addQc(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);

            Node userNode = User.getUserNode(jsonNode.get("email").asText(), graphDb);
            Node datasetNode = Framework.findDatasetNode(jsonNode.get("sampleId").asText(), jsonNode.get("worklistId").asText(), jsonNode.get("seqId").asText(), graphDb);

            //check dataset does not already have outstanding auths
            Node lastEventNode = Event.getLastUserEventNode(datasetNode, graphDb);

            if (lastEventNode.getId() != datasetNode.getId()){
                Event.UserEventStatus status = Event.getUserEventStatus(lastEventNode, graphDb);

                if (status == Event.UserEventStatus.PENDING_AUTH){
                    throw new IllegalArgumentException("Cannot add QC result. Auth pending.");
                }

            }

            //add event
            try (Transaction tx = graphDb.beginTx()) {
                Node newEventNode = graphDb.createNode(Labels.qualityControl);

                //add properties
                newEventNode.setProperty("passOrFail", jsonNode.get("passOrFail").asBoolean());

                if (jsonNode.has("evidence") && !jsonNode.get("evidence").asText().equals("")) {
                    newEventNode.setProperty("evidence", jsonNode.get("evidence").asText());
                }

                Relationship addedByRelationship = newEventNode.createRelationshipTo(userNode, Relationships.addedBy);
                addedByRelationship.setProperty("date", new Date().getTime());

                lastEventNode.createRelationshipTo(newEventNode, Relationships.hasEvent);

                tx.success();
            }

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

}
