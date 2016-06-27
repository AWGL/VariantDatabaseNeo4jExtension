package nhs.genetics.cardiff.variantdatabase.plugin;

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
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * A class for reporting variants
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
@Path("/variantdatabase/report")
public class Report {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateFormat dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");

    public Report(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    /**
     * @return Creates text report of selected variants and annotations
     * @param json {sampleId, worklistId, seqId, email, variant:[variant]}
     */
    @POST
    @Path("/text")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response report(final String json) {
        //TODO

        try {
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    //force windows NL
                    PrintWriter printWriter = new PrintWriter(os){
                        @Override
                        public void println(String x){
                            print(x + "\r\n");
                        }
                    };

                    JsonNode jsonNode = objectMapper.readTree(json);

                    Node datasetNode = Framework.findDatasetNode(jsonNode.get("sampleId").asText(), jsonNode.get("worklistId").asText(), jsonNode.get("seqId").asText(), graphDb);
                    Node userNode = graphDb.findNode(Labels.user, "email", jsonNode.get("email").asText());

                    printWriter.println("hi\ttab\ttab\tnewline");

                    printWriter.flush();
                    printWriter.close();
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
