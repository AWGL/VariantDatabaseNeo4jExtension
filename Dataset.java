package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
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
     * GET /variantdatabase/dataset/info
     * Returns all QC passing analyses
     */
    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public Response info() {

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

                                        //write analyses
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

    @GET
    @Path("/qualitycontrol")
    @Produces(MediaType.APPLICATION_JSON)
    public Response qualityControl() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> iter = graphDb.findNodes(Labels.qualityControl)){

                            while (iter.hasNext()) {
                                Node qualityControlNode = iter.next();

                                if (Event.getUserEventStatus(qualityControlNode, graphDb) == Event.UserEventStatus.PENDING_AUTH){

                                    Relationship addedByRelationship = qualityControlNode.getSingleRelationship(Relationships.addedBy, Direction.OUTGOING);
                                    Node addedByUserNode = addedByRelationship.getEndNode();

                                    jg.writeStartObject();

                                    jg.writeObjectFieldStart("event");
                                    Framework.writeNodeProperties(qualityControlNode.getId(), qualityControlNode.getAllProperties(), qualityControlNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("add");
                                    User.writeLiteUserRecord(addedByUserNode.getId(), addedByUserNode.getLabels(), addedByUserNode.getProperty("fullName").toString(), addedByUserNode.getProperty("email").toString(), (boolean) addedByUserNode.getProperty("admin"), jg);
                                    Framework.writeNodeProperties(addedByUserNode.getId(), addedByUserNode.getAllProperties(), addedByUserNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeNumberField("eventNodeId", qualityControlNode.getId());
                                    jg.writeStringField("event", "Quality Control");
                                    jg.writeBooleanField("value", (boolean) qualityControlNode.getProperty("passOrFail"));
                                    if (qualityControlNode.hasProperty("evidence")) jg.writeStringField("evidence", qualityControlNode.getProperty("evidence").toString());

                                    jg.writeObjectFieldStart("add");
                                    User.writeLiteUserRecord(addedByRelationship.getEndNode().getId(), addedByRelationship.getEndNode().getLabels(), addedByRelationship.getEndNode().getProperty("fullName").toString(), addedByRelationship.getEndNode().getProperty("email").toString(), (boolean) addedByRelationship.getEndNode().getProperty("admin"), jg);
                                    jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                                    jg.writeEndObject();

                                    Node runInfoNode = Event.getSubjectNodeFromEventNode(qualityControlNode, graphDb);

                                    jg.writeObjectFieldStart("dataset");
                                    Framework.writeNodeProperties(runInfoNode.getId(), runInfoNode.getAllProperties(), runInfoNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("sample");
                                    Node sampleNode = runInfoNode.getSingleRelationship(Relationships.hasData, Direction.INCOMING).getStartNode();
                                    Framework.writeNodeProperties(sampleNode.getId(), sampleNode.getAllProperties(), sampleNode.getLabels(), jg);
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
