package moonset.metastore.sync.util;

import org.apache.hadoop.hive.metastore.api.Table;

public final class TableUtils {
    /** Hive use this parameter to judge if the table is an external table. */
    public static final String EXTERNAL_PARAM = "EXTERNAL";

    /** Store the create time in table parameter, sice this field will be overriden when createTable(). */
    public static final String ORIGINAL_CREATE_TIME = "original_create_time";

    private static final String EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";


    /**
     * Hive use this parameter to judge if the table is an external table. But AWS DataCatalog lack
     * this parameter.
     */
    public static Table rewriteExternalFlag(Table table) {
        if (table.getParameters().containsKey(EXTERNAL_PARAM)) return table;
        Table modifiedTable = table.deepCopy();
        if (modifiedTable.isSetTableType()) {
            modifiedTable.putToParameters(
                    EXTERNAL_PARAM,
                    EXTERNAL_TABLE_TYPE.equals(modifiedTable.getTableType()) ? "TRUE" : "FALSE");
        }
        return modifiedTable;
    }
}
