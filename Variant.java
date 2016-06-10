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
     * POST {variantId:"variantId"} /variantdatabase/variant/add
     * Adds new variant; must be in minimum representation
     */
    @POST
    @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(final String json){

        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            try (Transaction tx = graphDb.beginTx()) {
                Node node = graphDb.findNode(Labels.variant, "variantId", jsonNode.get("variantId").asText());

                if (node == null){
                    Node variantNode = graphDb.createNode(Labels.variant);
                    variantNode.addLabel(Labels.annotate); //add to annotation queue
                    variantNode.setProperty("variantId", jsonNode.get("variantId").asText());
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
     * POST {variantId} /variantdatabase/variant/info
     * Returns variant and annotations
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
     * POST {variantId:"variantId"} to /variantdatabase/variant/observations
     * Returns observations of a variant
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
     * POST {variantId} /variantdatabase/variant/annotation
     * Returns variant annotations
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

}
