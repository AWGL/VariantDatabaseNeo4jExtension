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
 * A class for working with panels
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/panel")
public class Panel {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Panel(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * List all panels
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
                        try (ResourceIterator<Node> panelNodes = graphDb.findNodes(Labels.panel)) {

                            while (panelNodes.hasNext()) {
                                Node panelNode = panelNodes.next();

                                Relationship designedByRelationship = panelNode.getSingleRelationship(Relationships.designedBy, Direction.OUTGOING);
                                Node userNode = designedByRelationship.getEndNode();

                                jg.writeStartObject();

                                jg.writeFieldName("panel");
                                Framework.writeNodeProperties(panelNode.getId(), panelNode.getAllProperties(), panelNode.getLabels(), jg);
                                jg.writeEndObject();

                                jg.writeFieldName("designedBy");
                                Framework.writeRelationshipProperties(designedByRelationship.getId(), designedByRelationship.getAllProperties(), designedByRelationship.getType().name(), jg);
                                jg.writeEndObject();

                                jg.writeFieldName("user");
                                Framework.writeNodeProperties(userNode.getId(), userNode.getAllProperties(), userNode.getLabels(), jg);
                                jg.writeEndObject();

                                jg.writeEndObject();

                            }

                            panelNodes.close();
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
     * Returns panel info
     * @param json {panelId}
     */
    @POST
    @Path("/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response info(String json) {

        try {
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    JsonNode jsonNode = objectMapper.readTree(json);

                    jg.writeStartObject();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node panelNode = graphDb.findNode(Labels.panel, "panelId", jsonNode.get("panelId").asText());

                        Relationship designedByRelationship = panelNode.getSingleRelationship(Relationships.designedBy, Direction.OUTGOING);
                        Node userNode = designedByRelationship.getEndNode();

                        jg.writeFieldName("panel");
                        Framework.writeNodeProperties(panelNode.getId(), panelNode.getAllProperties(), panelNode.getLabels(), jg);
                        jg.writeEndObject();

                        jg.writeFieldName("designedBy");
                        Framework.writeRelationshipProperties(designedByRelationship.getId(), designedByRelationship.getAllProperties(), designedByRelationship.getType().name(), jg);
                        jg.writeEndObject();

                        jg.writeFieldName("user");
                        Framework.writeNodeProperties(userNode.getId(), userNode.getAllProperties(), userNode.getLabels(), jg);
                        jg.writeEndObject();

                        jg.writeArrayFieldStart("symbols");

                        for (Relationship containsSymbol : panelNode.getRelationships(Direction.OUTGOING, Relationships.containsSymbol)){
                            Node symbolNode = containsSymbol.getEndNode();

                            jg.writeStartObject();
                            Framework.writeNodeProperties(symbolNode.getId(), symbolNode.getAllProperties(), symbolNode.getLabels(), jg);
                            jg.writeEndObject();

                        }

                        jg.writeEndArray();
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
     * Adds new gene panel
     * @param json {email, list[symbolId], panelId}
     */
    @POST
    @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email").asText());

                //make panel node
                Node panelNode = graphDb.createNode(Labels.panel);
                panelNode.setProperty("panelId", jsonNode.get("panelId").asText());

                //link to user
                Relationship designedByRelationship = panelNode.createRelationshipTo(userNode, Relationships.designedBy);
                designedByRelationship.setProperty("date", new Date().getTime());

                //link to genes
                for (JsonNode symbol : jsonNode.get("list")) {
                    Node symbolNode = Framework.matchOrCreateUniqueNode(graphDb, Labels.symbol, "symbolId", symbol); //match or create gene
                    panelNode.createRelationshipTo(symbolNode, Relationships.containsSymbol); //link to panel
                }

                tx.success();
            }

            return Response
                    .status(Response.Status.OK)
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
