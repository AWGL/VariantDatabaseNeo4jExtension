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
     * Workflow annotation for database endpoints
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface WorkflowAnnotation {
        String name();
        String description();
    }

    /**
     * @return Returns all variants stratified for rareness in variant frequency populations
     * @param json {sampleId, worklistId, seqId}
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
                    int class1Calls = 0, not1KGRareVariants = 0, notExACRareVariants = 0, passVariants = 0, total = 0;

                    jg.writeStartObject();
                    jg.writeArrayFieldStart("variants");

                    try (Transaction tx = graphDb.beginTx()) {
                        for (Relationship relationship : datasetNode.getRelationships(Direction.OUTGOING)){

                            //skip non variant nodes
                            if (!relationship.isType(Relationships.hasHetVariant) && !relationship.isType(Relationships.hasHomVariant)){
                                continue;
                            }

                            Node variantNode = relationship.getEndNode();

                            jg.writeStartObject();

                            jg.writeObjectFieldStart("variant");
                            Framework.writeNodeProperties(variantNode.getId(), variantNode.getAllProperties(), variantNode.getLabels(), jg);
                            jg.writeEndObject();

                            jg.writeObjectFieldStart("inheritance");
                            Framework.writeRelationshipProperties(relationship.getId(), relationship.getAllProperties(), relationship.getType().name(), jg);
                            jg.writeEndObject();

                            //stratify variants
                            Node lastActiveEventNode = Event.getLastActiveUserEventNode(variantNode, graphDb);
                            if (lastActiveEventNode != null && lastActiveEventNode.hasProperty("classification")) {

                                if ((int) lastActiveEventNode.getProperty("classification") == 1){
                                    jg.writeNumberField("filter", 0);
                                    class1Calls++;
                                } else {
                                    jg.writeNumberField("filter", 3);
                                    passVariants++;
                                }

                            } else if (!isExACRareVariant(variantNode, 0.01)) {
                                jg.writeNumberField("filter", 1);
                                notExACRareVariants++;
                            } else if (!is1KGRareVariant(variantNode, 0.01)) {
                                jg.writeNumberField("filter", 2);
                                not1KGRareVariants++;
                            } else {
                                jg.writeNumberField("filter", 3);
                                passVariants++;
                            }

                            total++;

                            jg.writeEndObject();

                        }
                    }

                    jg.writeEndArray();

                    //write filters
                    jg.writeFieldName("filters");
                    jg.writeStartArray();

                    jg.writeStartObject();
                    jg.writeStringField("key", "Class 1");
                    jg.writeNumberField("y", class1Calls);
                    jg.writeEndObject();

                    jg.writeStartObject();
                    jg.writeStringField("key", "ExAC >1% Frequency");
                    jg.writeNumberField("y", notExACRareVariants);
                    jg.writeEndObject();

                    jg.writeStartObject();
                    jg.writeStringField("key", "1KG >1% Frequency");
                    jg.writeNumberField("y", not1KGRareVariants);
                    jg.writeEndObject();

                    jg.writeStartObject();
                    jg.writeStringField("key", "Pass");
                    jg.writeNumberField("y", passVariants);
                    jg.writeEndObject();

                    jg.writeEndArray();

                    jg.writeNumberField("total", total);

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
     * @return Returns info about available variant filter workflows
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

    private boolean is1KGRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {

                if (variantNode.hasProperty("kGPhase3" + population.toString() + "Af")){
                    if ((float) variantNode.getProperty("kGPhase3" + population.toString() + "Af") > maxAlleleFrequency){
                        return false;
                    }
                }

            }

        }

        return true;
    }

    private boolean isExACRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {

                if (variantNode.hasProperty("exac" + population.toString() + "Af")){
                    if ((float) variantNode.getProperty("exac" + population.toString() + "Af") > maxAlleleFrequency){
                        return false;
                    }
                }

            }

        }

        return true;
    }

}
