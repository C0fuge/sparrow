package sparrow.metabase;

/**
 * A column of a table or of a query result, as the driver hands it to Metabase.
 *
 * @param name column name
 * @param databaseType DuckDB type name (see {@link DuckDbTypes})
 * @param position zero-based position
 * @param nullable whether the column may hold nulls
 */
public record Column(String name, String databaseType, int position, boolean nullable) {
}
