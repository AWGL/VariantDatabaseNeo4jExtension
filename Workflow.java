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
import java.util.Arrays;
import java.util.HashSet;

/**
 * A class for filtering variants
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-04-16
 */
//@Path("/variantdatabase/workflow") TODO
public class Workflow {

    private final GraphDatabaseService graphDb;
    private final Log log;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public Workflow(@Context GraphDatabaseService graphDb, @Context Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

}
