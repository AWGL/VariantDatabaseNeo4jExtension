package nhs.genetics.cardiff.variantdatabase.plugin;

/**
 * A exception class for identifying nodes that already exist
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-02-16
 */
public class NodeAlreadyExists extends RuntimeException {

    public NodeAlreadyExists(String message) {
        super(message);
    }
    public String getMessage()
    {
        return super.getMessage();
    }

}
