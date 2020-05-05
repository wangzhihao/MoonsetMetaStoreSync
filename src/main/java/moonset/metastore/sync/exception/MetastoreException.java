package moonset.metastore.sync.exception;
/** The exception for metastore sync process. */
public class MetastoreException extends Exception {

    /**
     * Constructor for only message.
     *
     * @param message message.
     */
    public MetastoreException(final String message) {
        super(message);
    }

    /**
     * Constructor for message and throwable.
     *
     * @param message message.
     * @param throwable throwable.
     */
    public MetastoreException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
