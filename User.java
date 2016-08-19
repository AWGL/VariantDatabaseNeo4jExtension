package nhs.genetics.cardiff.variantdatabase.plugin;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * A class for working with users
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/user")
public class User {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public User(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * @return Returns info about a user
     * @param json {email}
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
                        Node userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email").asText().toLowerCase());

                        jg.writeObjectFieldStart("user");
                        Framework.writeNodeProperties(userNode.getId(), userNode.getAllProperties(), userNode.getLabels(), jg);
                        jg.writeEndObject();
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
                    .entity((e.getLocalizedMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    /**
     * Overwrite user password
     * @param json {email, password}
     * @return response code
     */
    @POST
    @Path("/update/password")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updatePassword(final String json) {

        try {

            JsonNode jsonNode = objectMapper.readTree(json);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email").asText().toLowerCase());
                userNode.setProperty("password", jsonNode.get("password").asText());
                tx.success();
            }

            return Response.status(Response.Status.OK).build();

        } catch (Exception e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getLocalizedMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    /**
     * Adds new user
     * @param json {email, fullName, password, admin}
     * @return response code
     */
    @POST
    @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(final String json) {

        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.createNode(Labels.user);

                userNode.setProperty("fullName", jsonNode.get("fullName").asText());
                userNode.setProperty("password", jsonNode.get("password").asText());
                userNode.setProperty("email", jsonNode.get("email").asText().toLowerCase());
                userNode.setProperty("admin", jsonNode.get("admin").asBoolean());

                tx.success();
            }

            return Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getLocalizedMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

}
