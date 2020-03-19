package moonset.metastore.sync.parser;

import java.util.ListIterator;
import org.apache.commons.cli.*;

/**
 * This class is a wrapper of GnuParser with feature to ignore unknowning options. Code is from
 * https://stackoverflow.com/a/8613949/1494097
 */
public class ExtendedGnuParser extends GnuParser {

    private boolean ignoreUnrecognizedOption;

    public ExtendedGnuParser(final boolean ignoreUnrecognizedOption) {
        this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
    }

    @Override
    protected void processOption(final String arg, final ListIterator iter) throws ParseException {
        boolean hasOption = getOptions().hasOption(arg);

        if (hasOption || !ignoreUnrecognizedOption) {
            super.processOption(arg, iter);
        }
    }
}
