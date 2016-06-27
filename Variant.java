package nhs.genetics.cardiff.variantdatabase.plugin;

import nhs.genetics.cardiff.framework.GenomeVariant;
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
import java.util.HashMap;

/**
 * A class for working with variants
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/variant")
public class Variant {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Variant(@Context GraphDatabaseService graphDb, @Context Log log){
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * Adds new variant
     * @param json {variantId}
     */
    //TODO check variant is correct: check ref/alt/pos etc
    @POST
    @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(final String json){

        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            GenomeVariant genomeVariant = new GenomeVariant(jsonNode.get("variantId").asText());
            genomeVariant.convertToMinimalRepresentation();

            try (Transaction tx = graphDb.beginTx()) {
                Node node = graphDb.findNode(Labels.variant, "variantId", genomeVariant.toString());

                if (node == null){
                    Node variantNode = graphDb.createNode(Labels.variant);
                    variantNode.addLabel(Labels.annotate); //add to annotation queue
                    variantNode.setProperty("variantId", genomeVariant.toString());
                }

                tx.success();
            }

            return Response.ok().build();
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    /**
     * Returns variant and genomic annotations
     * @param json {variantId}
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
                        Node variantNode = graphDb.findNode(Labels.variant, "variantId", jsonNode.get("variantId").asText());
                        Framework.writeNodeProperties(variantNode.getId(), variantNode.getAllProperties(), variantNode.getLabels(), jg);
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
     * Returns observations/counts of a variant
     * @param json {variantId}
     */
    @POST
    @Path("/observations")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response counts(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    JsonNode jsonNode = objectMapper.readTree(json);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node variantNode = graphDb.findNode(Labels.variant, "variantId", jsonNode.get("variantId").asText());

                        for (Relationship inheritanceRel : variantNode.getRelationships(Direction.INCOMING)) {
                            Node datasetNode = inheritanceRel.getStartNode();

                            if (!datasetNode.hasLabel(Labels.dataset)){
                                continue;
                            }

                            //check if run has passed QC
                            Node qcNode = Event.getLastActiveUserEventNode(datasetNode, graphDb);
                            if (qcNode == null || !(boolean) qcNode.getProperty("passOrFail")){
                                continue;
                            }

                            if (datasetNode.hasLabel(Labels.dataset)){
                                Node sampleNode = datasetNode.getSingleRelationship(Relationships.hasData, Direction.INCOMING).getStartNode();

                                if (sampleNode.hasLabel(Labels.sample)){
                                    jg.writeStartObject();


                                    jg.writeStringField("inheritance", Relationships.getVariantInheritance(inheritanceRel.getType().name()));

                                    jg.writeObjectFieldStart("sample");
                                    Framework.writeNodeProperties(sampleNode.getId(), sampleNode.getAllProperties(), sampleNode.getLabels(), jg);
                                    jg.writeEndObject();

                                    jg.writeObjectFieldStart("datasets");
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
     * Returns all variant annotations
     * @param json {variantId}
     */
    @POST
    @Path("/annotation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response annotation(final String json) {

        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node variantNode = graphDb.findNode(Labels.variant, "variantId", jsonNode.get("variantId").asText());

                        for (Relationship relationship : variantNode.getRelationships(Direction.OUTGOING, Relationships.hasAnnotation)){
                            Node featureNode = relationship.getEndNode();
                            Node symbolNode = featureNode.getSingleRelationship(Relationships.hasFeature, Direction.INCOMING).getStartNode();

                            jg.writeStartObject();
                            Framework.writeRelationshipProperties(relationship.getId(), relationship.getAllProperties(), relationship.getType().name(), jg);

                            jg.writeObjectFieldStart("feature");
                            Framework.writeNodeProperties(featureNode.getId(), featureNode.getAllProperties(), featureNode.getLabels(), jg);
                            jg.writeEndObject();

                            jg.writeObjectFieldStart("symbol");
                            Framework.writeNodeProperties(symbolNode.getId(), symbolNode.getAllProperties(), symbolNode.getLabels(), jg);
                            jg.writeEndObject();

                            jg.writeEndObject();
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
     * Adds variant pathogenicity. class must be in range 1-5.
     * @param json {variantId,userId,classification,evidence}
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

            //add event
            try (Transaction tx = graphDb.beginTx()) {
                Node newEventNode = graphDb.createNode(Labels.pathogenicity);

                //add properties
                newEventNode.setProperty("classification", classification);

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
