package sparrow.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TPC-H data written by DuckDB without its {@code tpch} extension (not bundled with the JDBC driver, and CI
 * must not depend on downloading it): the eight tables with the TPC-H schema, cardinalities, key relations
 * (four suppliers per part, lineitem suppliers among the part's suppliers, a third of the customers without
 * orders), value domains and word lists of {@code dbgen}, filled with deterministic hash-based values. Not
 * byte-identical to {@code dbgen} output; the tests compare against DuckDB on the same files, so every query
 * predicate only needs to select a non-trivial part of the data. Each table is written as
 * {@code <dir>/tpch/<table>/part-<i>.parquet}, the large ones in several files so that every cluster node
 * reads a part.
 */
final class TpchData {

    /** The tables, in dependency order. */
    static final List<String> TABLES = List.of("region", "nation", "supplier", "part", "partsupp", "customer",
            "orders", "lineitem");

    /** Tables written as four files, by key modulo 4. */
    private static final Map<String, String> SPLIT_BY = Map.of("part", "p_partkey", "partsupp", "ps_partkey",
            "customer", "c_custkey", "orders", "o_orderkey", "lineitem", "l_orderkey");

    private static final List<String> COLORS = List.of("almond", "antique", "aquamarine", "azure", "beige",
            "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse",
            "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim",
            "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod",
            "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon",
            "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty",
            "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink",
            "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy",
            "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato",
            "turquoise", "violet", "wheat", "white", "yellow");
    private static final List<String> WORDS = List.of("furiously", "quickly", "carefully", "blithely", "slyly",
            "fluffily", "regular", "special", "pending", "final", "express", "ironic", "bold", "even", "silent",
            "unusual", "requests", "deposits", "packages", "accounts", "theodolites", "instructions", "foxes",
            "pinto", "beans", "ideas", "asymptotes", "dependencies", "excuses", "platelets", "sleep", "wake",
            "haggle", "nag", "use", "boost", "affix", "detect", "integrate", "cajole", "about", "above", "against",
            "along", "among");

    private TpchData() {
    }

    /** Writes the tables at scale factor {@code sf} (rows: SF 0.1 has 600 k lineitems). */
    static void write(Path dir, double sf) throws Exception {
        long suppliers = Math.round(10_000 * sf);
        long parts = Math.round(200_000 * sf);
        long customers = Math.round(150_000 * sf);
        long orders = Math.round(1_500_000 * sf);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            // r(n, ...): pseudo-random integer in [0, n) from the other arguments
            s.execute("CREATE MACRO r(n, a, b, c) AS (hash(a, b, c) % n)::BIGINT");
            s.execute("CREATE MACRO pick(l, a, b, c) AS l[r(len(l), a, b, c) + 1]");
            s.execute("CREATE MACRO words(a, b, n) AS array_to_string(list_transform(range(n),"
                    + " i -> pick(" + list(WORDS) + ", a, b, i)), ' ')");
            s.execute("CREATE MACRO phone(k, b, nation) AS (nation + 10)::VARCHAR || '-' || (100 + r(900, k, b, 1))"
                    + " || '-' || (100 + r(900, k, b, 2)) || '-' || (1000 + r(9000, k, b, 3))");
            s.execute("CREATE MACRO money(lo, hi, a, b) AS ((lo + r(hi - lo + 1, a, b, 0)) / 100)::DECIMAL(15, 2)");
            // dbgen: p_retailprice from the key; ps_suppkey / l_suppkey: supplier i (0..3) of a part
            s.execute("CREATE MACRO price(k) AS ((90000 + (k // 10) % 20001 + 100 * (k % 1000)) / 100)"
                    + "::DECIMAL(15, 2)");
            s.execute("CREATE MACRO supp(k, i) AS ((k + i * (" + suppliers + " // 4 + (k - 1) // " + suppliers
                    + ")) % " + suppliers + " + 1)::INTEGER");

            s.execute("CREATE TABLE region AS SELECT k::INTEGER AS r_regionkey, name AS r_name,"
                    + " words(k, 1, 8) AS r_comment FROM (VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'),"
                    + " (3, 'EUROPE'), (4, 'MIDDLE EAST')) t(k, name)");
            s.execute("CREATE TABLE nation AS SELECT k::INTEGER AS n_nationkey, name AS n_name,"
                    + " region::INTEGER AS n_regionkey, words(k, 2, 8) AS n_comment FROM (VALUES (0, 'ALGERIA', 0),"
                    + " (1, 'ARGENTINA', 1), (2, 'BRAZIL', 1), (3, 'CANADA', 1), (4, 'EGYPT', 4), (5, 'ETHIOPIA', 0),"
                    + " (6, 'FRANCE', 3), (7, 'GERMANY', 3), (8, 'INDIA', 2), (9, 'INDONESIA', 2), (10, 'IRAN', 4),"
                    + " (11, 'IRAQ', 4), (12, 'JAPAN', 2), (13, 'JORDAN', 4), (14, 'KENYA', 0), (15, 'MOROCCO', 0),"
                    + " (16, 'MOZAMBIQUE', 0), (17, 'PERU', 1), (18, 'CHINA', 2), (19, 'ROMANIA', 3),"
                    + " (20, 'SAUDI ARABIA', 4), (21, 'VIETNAM', 2), (22, 'RUSSIA', 3), (23, 'UNITED KINGDOM', 3),"
                    + " (24, 'UNITED STATES', 1)) t(k, name, region)");
            s.execute("CREATE TABLE supplier AS SELECT k::INTEGER AS s_suppkey,"
                    + " 'Supplier#' || lpad(k::VARCHAR, 9, '0') AS s_name, words(k, 10, 3) AS s_address,"
                    + " nation::INTEGER AS s_nationkey, phone(k, 11, nation) AS s_phone,"
                    + " money(-99999, 999999, k, 12) AS s_acctbal,"
                    + " CASE WHEN k % 97 = 5 THEN words(k, 13, 2) || ' Customer ' || words(k, 14, 1)"
                    + " || ' Complaints ' || words(k, 15, 2)"
                    + " WHEN k % 97 = 50 THEN words(k, 13, 2) || ' Customer ' || words(k, 14, 1) || ' Recommends'"
                    + " ELSE words(k, 13, 7) END AS s_comment"
                    + " FROM (SELECT range AS k, r(25, range, 16, 0) AS nation FROM range(1, " + (suppliers + 1)
                    + "))");
            s.execute("CREATE TABLE part AS SELECT k::INTEGER AS p_partkey, array_to_string(list_transform(range(5),"
                    + " i -> pick(" + list(COLORS) + ", k, 20, i)), ' ') AS p_name,"
                    + " 'Manufacturer#' || m AS p_mfgr, 'Brand#' || m || (1 + r(5, k, 21, 0)) AS p_brand,"
                    + " pick(['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'], k, 22, 0) || ' '"
                    + " || pick(['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED'], k, 22, 1) || ' '"
                    + " || pick(['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'], k, 22, 2) AS p_type,"
                    + " (1 + r(50, k, 23, 0))::INTEGER AS p_size,"
                    + " pick(['SM', 'LG', 'MED', 'JUMBO', 'WRAP'], k, 24, 0) || ' '"
                    + " || pick(['CASE', 'BOX', 'BAG', 'JAR', 'PKG', 'PACK', 'CAN', 'DRUM'], k, 24, 1) AS p_container,"
                    + " price(k) AS p_retailprice, words(k, 25, 3) AS p_comment"
                    + " FROM (SELECT range AS k, 1 + r(5, range, 26, 0) AS m FROM range(1, " + (parts + 1) + "))");
            s.execute("CREATE TABLE partsupp AS SELECT k::INTEGER AS ps_partkey, supp(k, i) AS ps_suppkey,"
                    + " (1 + r(9999, k, i, 30))::INTEGER AS ps_availqty, money(100, 100000, k * 4 + i, 31)"
                    + " AS ps_supplycost, words(k * 4 + i, 32, 8) AS ps_comment"
                    + " FROM range(1, " + (parts + 1) + ") p(k), range(4) s(i)");
            s.execute("CREATE TABLE customer AS SELECT k::INTEGER AS c_custkey,"
                    + " 'Customer#' || lpad(k::VARCHAR, 9, '0') AS c_name, words(k, 40, 3) AS c_address,"
                    + " nation::INTEGER AS c_nationkey, phone(k, 41, nation) AS c_phone,"
                    + " money(-99999, 999999, k, 42) AS c_acctbal,"
                    + " pick(['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD'], k, 43, 0)"
                    + " AS c_mktsegment, words(k, 44, 8) AS c_comment"
                    + " FROM (SELECT range AS k, r(25, range, 45, 0) AS nation FROM range(1, " + (customers + 1)
                    + "))");
            // orders without status and total price (both follow from the lineitems); custkey % 3 != 0
            s.execute("CREATE TABLE o AS SELECT k::BIGINT AS o_orderkey,"
                    + " (CASE WHEN c % 3 = 0 THEN c - 1 ELSE c END)::INTEGER AS o_custkey,"
                    + " (DATE '1992-01-01' + r(2406, k, 50, 0)::INTEGER) AS o_orderdate,"
                    + " pick(['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'], k, 51, 0)"
                    + " AS o_orderpriority, 'Clerk#' || lpad((1 + r(" + Math.max(1, Math.round(1000 * sf))
                    + ", k, 52, 0))::VARCHAR, 9, '0') AS o_clerk, 0 AS o_shippriority, words(k, 53, 6) AS o_comment,"
                    + " (1 + r(7, k, 54, 0))::INTEGER AS lines"
                    + " FROM (SELECT range AS k, 1 + r(" + customers + ", range, 55, 0) AS c FROM range(1, "
                    + (orders + 1) + "))");
            s.execute("CREATE TABLE lineitem AS SELECT l_orderkey, l_partkey, supp(l_partkey, r(4, l_orderkey,"
                    + " l_linenumber, 60))::INTEGER AS l_suppkey, l_linenumber, l_quantity,"
                    + " (l_quantity * price(l_partkey))::DECIMAL(15, 2) AS l_extendedprice, l_discount, l_tax,"
                    + " CASE WHEN l_receiptdate <= DATE '1995-06-17'"
                    + " THEN pick(['R', 'A'], l_orderkey, l_linenumber, 61) ELSE 'N' END AS l_returnflag,"
                    + " CASE WHEN l_shipdate > DATE '1995-06-17' THEN 'O' ELSE 'F' END AS l_linestatus,"
                    + " l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM ("
                    + "SELECT *, (l_shipdate + (1 + r(30, l_orderkey, l_linenumber, 62))::INTEGER) AS l_receiptdate"
                    + " FROM (SELECT o_orderkey AS l_orderkey, l_linenumber::INTEGER AS l_linenumber,"
                    + " (1 + r(" + parts + ", o_orderkey, l_linenumber, 63))::INTEGER AS l_partkey,"
                    + " (1 + r(50, o_orderkey, l_linenumber, 64))::DECIMAL(15, 2) AS l_quantity,"
                    + " (r(11, o_orderkey, l_linenumber, 65) / 100)::DECIMAL(15, 2) AS l_discount,"
                    + " (r(9, o_orderkey, l_linenumber, 66) / 100)::DECIMAL(15, 2) AS l_tax,"
                    + " (o_orderdate + (1 + r(121, o_orderkey, l_linenumber, 67))::INTEGER) AS l_shipdate,"
                    + " (o_orderdate + (30 + r(61, o_orderkey, l_linenumber, 68))::INTEGER) AS l_commitdate,"
                    + " pick(['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'], o_orderkey,"
                    + " l_linenumber, 69) AS l_shipinstruct,"
                    + " pick(['REG AIR', 'AIR', 'RAIL', 'SHIP', 'TRUCK', 'MAIL', 'FOB'], o_orderkey, l_linenumber, 70)"
                    + " AS l_shipmode, words(o_orderkey * 8 + l_linenumber, 71, 4) AS l_comment"
                    + " FROM (SELECT o_orderkey, o_orderdate, unnest(range(1, lines + 1)) AS l_linenumber FROM o)))");
            s.execute("CREATE TABLE orders AS SELECT o.o_orderkey, o_custkey, CASE WHEN f = n THEN 'F'"
                    + " WHEN f = 0 THEN 'O' ELSE 'P' END AS o_orderstatus, total AS o_totalprice, o_orderdate,"
                    + " o_orderpriority, o_clerk, o_shippriority::INTEGER AS o_shippriority, o_comment FROM o JOIN ("
                    + "SELECT l_orderkey, count(*) AS n, count(*) FILTER (WHERE l_linestatus = 'F') AS f,"
                    + " sum(l_extendedprice * (1 + l_tax) * (1 - l_discount))::DECIMAL(15, 2) AS total"
                    + " FROM lineitem GROUP BY l_orderkey) l ON o.o_orderkey = l.l_orderkey");

            for (String table : TABLES) {
                Path tableDir = Files.createDirectories(dir.resolve("tpch").resolve(table));
                String key = SPLIT_BY.get(table);
                int files = key == null ? 1 : 4;
                for (int f = 0; f < files; f++) {
                    String where = key == null ? "" : " WHERE " + key + " % " + files + " = " + f;
                    s.execute("COPY (SELECT * FROM " + table + where + " ORDER BY ALL) TO '"
                            + tableDir.resolve("part-" + f + ".parquet") + "' (FORMAT parquet)");
                }
            }
        }
    }

    private static String list(List<String> values) {
        return values.stream().map(v -> "'" + v + "'").collect(Collectors.joining(", ", "[", "]"));
    }
}
