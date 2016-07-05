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
import java.util.HashSet;

/**
 * A class for managing Neo4j DB
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-03-19
 */
@Path("/variantdatabase/system")
public class System {
    private final Log log;
    private final GraphDatabaseService graphDb;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public System(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * @return Returns nodes with multiple relationships to the same node (which violates model)
     */
    @GET
    @Path("/multiple/relationships")
    @Produces(MediaType.APPLICATION_JSON)
    public Response diagnosticNodesMultipleRelationships() {
        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    HashSet<Long> ids = new HashSet<>();
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    try (Transaction tx = graphDb.beginTx()) {
                        for (Node node : graphDb.getAllNodes()){

                            for (Relationship relationship : node.getRelationships(Direction.OUTGOING)){
                                if (ids.contains(relationship.getEndNode().getId())) {
                                    log.debug("node " + node.getId() + " " + node.getLabels().toString() + " is connected to node " + relationship.getEndNode().getId() + " " + relationship.getEndNode().getLabels().toString() + " more than once");
                                } else {
                                    ids.add(relationship.getEndNode().getId());
                                }
                            }

                            ids.clear();
                        }
                    }

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
     * @return Returns request. For debugging
     * @param req Request
     */
    @POST
    @Path("/ret")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response ret(String req){
        try {
            log.info("Request: " + req);
            return Response.ok().entity(req).build();
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    /**
     * Loads all into cache
     * @return response code
     * */
    @GET
    @Path("/warmup")
    @Produces(MediaType.APPLICATION_JSON)
    public Response warmup(){
        try {

            try (Transaction tx = graphDb.beginTx()) {
                Node start;

                for (Node n : graphDb.getAllNodes()) {
                    n.getPropertyKeys();

                    for (Relationship relationship : n.getRelationships()) {
                        start = relationship.getStartNode();
                    }

                }

                for (Relationship r : graphDb.getAllRelationships()) {
                    r.getPropertyKeys();
                    start = r.getStartNode();
                }

                log.info("Warmed up!");
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
     * Creates indexes for first load
     * @return reponse code
     * */
    @GET
    @Path("/indexes")
    @Produces(MediaType.APPLICATION_JSON)
    public Response indexes(){
        try {

            try (Transaction tx = graphDb.beginTx()){

                graphDb.schema().constraintFor(Labels.user).assertPropertyIsUnique("email").create();
                graphDb.schema().constraintFor(Labels.disorder).assertPropertyIsUnique("disorderId").create();
                graphDb.schema().constraintFor(Labels.feature).assertPropertyIsUnique("featureId").create();
                graphDb.schema().constraintFor(Labels.panel).assertPropertyIsUnique("panelId").create();
                graphDb.schema().constraintFor(Labels.sample).assertPropertyIsUnique("sampleId").create();
                graphDb.schema().constraintFor(Labels.symbol).assertPropertyIsUnique("symbolId").create();
                graphDb.schema().constraintFor(Labels.variant).assertPropertyIsUnique("variantId").create();
                graphDb.schema().constraintFor(Labels.variant).assertPropertyIsUnique("dbSnpId").create();

                graphDb.schema().indexFor(Labels.dataset).on("worklistId");
                graphDb.schema().indexFor(Labels.dataset).on("seqId");
                graphDb.schema().indexFor(Labels.dataset).on("assay");
                graphDb.schema().indexFor(Labels.feature).on("ccdsId");

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


}

