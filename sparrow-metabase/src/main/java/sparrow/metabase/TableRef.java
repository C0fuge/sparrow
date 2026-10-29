package sparrow.metabase;

/**
 * A table the user can read.
 *
 * @param schema schema name
 * @param name table name
 * @param type table type as the server reports it ({@code TABLE}, {@code VIEW}, ...), or null
 */
public record TableRef(String schema, String name, String type) {
}
