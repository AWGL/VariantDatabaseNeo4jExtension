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
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

/**
 * A class for filtering variants
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/workflow")
public class Workflow {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Method[] workflows = this.getClass().getMethods();

    public Workflow(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * Workflow annotation for database
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface WorkflowAnnotation {
        String name();
        String description();
    }

    /**
     * POST {sampleId, worklistId, seqId} /variantdatabase/workflow/rare
     * Returns all variants stratified for rareness in varaint frequency populations
     */
    @POST
    @Path("/rare")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @WorkflowAnnotation(name = "Rare Variant Workflow v1", description = "A workflow to prioritise rare calls")
    public Response rareVariant(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Node datasetNode = Framework.findDatasetNode(jsonNode.get("sampleId").asText(), jsonNode.get("worklistId").asText(), jsonNode.get("seqId").asText(), graphDb);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        for (Relationship relationship : datasetNode.getRelationships(Direction.OUTGOING)){

                            //skip non variant nodes
                            if (!relationship.isType(Relationships.hasHetVariant) && !relationship.isType(Relationships.hasHetVariant)){
                                continue;
                            }


                            //todo

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
     * GET /variantdatabase/workflow/info
     * Returns info about available variant filter workflows
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

                    //print method names with get or post annotations
                    for (Method workflow : workflows){
                        if (workflow.isAnnotationPresent(WorkflowAnnotation.class) && !workflow.isAnnotationPresent(Deprecated.class)){
                            jg.writeStartObject();

                            jg.writeStringField("name", workflow.getAnnotation(WorkflowAnnotation.class).name());
                            jg.writeStringField("description", workflow.getAnnotation(WorkflowAnnotation.class).description());
                            jg.writeStringField("path", workflow.getAnnotation(javax.ws.rs.Path.class).value());

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

    private void filterVariantsByPanel(){
        //TODO
    }

}
