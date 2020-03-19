package moonset.metastore.sync.exception;

/** The exception for metastore connection process. */
public class MetastoreConnectionException extends Exception {

    /**
     * Constructor for only message.
     *
     * @param message message.
     */
    public MetastoreConnectionException(final String message) {
        super(message);
    }

    /**
     * Constructor for message and throwable.
     *
     * @param message message.
     * @param throwable throwable.
     */
    public MetastoreConnectionException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
