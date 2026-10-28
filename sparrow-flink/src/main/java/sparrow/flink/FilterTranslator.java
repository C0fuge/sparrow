package sparrow.flink;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import pro.surpin.data.arrowflight.client.expr.Expr;

/**
 * Translates Flink filter expressions into client-core {@link Expr} trees that the server evaluates exactly as
 * Flink would. Only column-vs-literal predicates on types whose comparison semantics are the same in Flink and
 * in the server's SQL (DuckDB) are translated: integers, DECIMAL, VARCHAR, BOOLEAN, DATE and TIMESTAMP
 * (microseconds). FLOAT/DOUBLE (NaN ordering differs), TIME, TIMESTAMP_LTZ, CHAR, binary types, casts and
 * functions stay in Flink.
 */
final class FilterTranslator {

    /** Not instantiable: static helpers only. */
    private FilterTranslator() {
    }

    /**
     * Translates one filter expression.
     *
     * @param expression resolved Flink expression
     * @return the equivalent server expression, or empty when it cannot be pushed exactly
     */
    static Optional<Expr> translate(ResolvedExpression expression) {
        if (expression instanceof FieldReferenceExpression field) {
            // A bare BOOLEAN column used as a predicate.
            return isType(field, LogicalTypeRoot.BOOLEAN)
                    ? Optional.of(new Expr.Compare(Expr.CompareOp.EQ, column(field), Expr.literal(Boolean.TRUE)))
                    : Optional.empty();
        }
        if (!(expression instanceof CallExpression call)) {
            return Optional.empty();
        }
        FunctionDefinition f = call.getFunctionDefinition();
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (f == BuiltInFunctionDefinitions.AND || f == BuiltInFunctionDefinitions.OR) {
            return logical(f == BuiltInFunctionDefinitions.AND, args);
        }
        if (f == BuiltInFunctionDefinitions.NOT && args.size() == 1) {
            return translate(args.get(0)).map(Expr.Not::new);
        }
        if ((f == BuiltInFunctionDefinitions.IS_NULL || f == BuiltInFunctionDefinitions.IS_NOT_NULL)
                && args.size() == 1 && args.get(0) instanceof FieldReferenceExpression field) {
            return Optional.of(new Expr.IsNull(column(field), f == BuiltInFunctionDefinitions.IS_NOT_NULL));
        }
        if (f == BuiltInFunctionDefinitions.IN) {
            return in(args);
        }
        if (f == BuiltInFunctionDefinitions.LIKE) {
            return like(args);
        }
        return compareOp(f).flatMap(op -> compare(op, args));
    }

    /**
     * Folds an n-ary AND/OR; every operand must translate.
     *
     * @param and true for AND, false for OR
     * @param args operands
     * @return the folded expression, or empty
     */
    private static Optional<Expr> logical(boolean and, List<ResolvedExpression> args) {
        if (args.size() < 2) {
            return Optional.empty();
        }
        Expr result = null;
        for (ResolvedExpression arg : args) {
            Optional<Expr> e = translate(arg);
            if (e.isEmpty()) {
                return Optional.empty();
            }
            result = result == null ? e.get() : and ? new Expr.And(result, e.get()) : new Expr.Or(result, e.get());
        }
        return Optional.of(result);
    }

    /**
     * Maps a comparison function to its operator.
     *
     * @param f function
     * @return operator, or empty for other functions
     */
    private static Optional<Expr.CompareOp> compareOp(FunctionDefinition f) {
        if (f == BuiltInFunctionDefinitions.EQUALS) {
            return Optional.of(Expr.CompareOp.EQ);
        } else if (f == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return Optional.of(Expr.CompareOp.NE);
        } else if (f == BuiltInFunctionDefinitions.LESS_THAN) {
            return Optional.of(Expr.CompareOp.LT);
        } else if (f == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return Optional.of(Expr.CompareOp.LE);
        } else if (f == BuiltInFunctionDefinitions.GREATER_THAN) {
            return Optional.of(Expr.CompareOp.GT);
        } else if (f == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return Optional.of(Expr.CompareOp.GE);
        }
        return Optional.empty();
    }

    /**
     * Translates {@code column op literal} or {@code literal op column} (operator mirrored).
     *
     * @param op operator
     * @param args two operands
     * @return comparison, or empty
     */
    private static Optional<Expr> compare(Expr.CompareOp op, List<ResolvedExpression> args) {
        if (args.size() != 2) {
            return Optional.empty();
        }
        if (args.get(0) instanceof FieldReferenceExpression field
                && args.get(1) instanceof ValueLiteralExpression value) {
            return literal(field, value).map(l -> new Expr.Compare(op, column(field), l));
        }
        if (args.get(1) instanceof FieldReferenceExpression field
                && args.get(0) instanceof ValueLiteralExpression value) {
            return literal(field, value).map(l -> new Expr.Compare(mirror(op), column(field), l));
        }
        return Optional.empty();
    }

    /**
     * Mirrors an operator for swapped operands ({@code 5 < a} is {@code a > 5}).
     *
     * @param op operator
     * @return mirrored operator
     */
    private static Expr.CompareOp mirror(Expr.CompareOp op) {
        return switch (op) {
            case LT -> Expr.CompareOp.GT;
            case LE -> Expr.CompareOp.GE;
            case GT -> Expr.CompareOp.LT;
            case GE -> Expr.CompareOp.LE;
            default -> op;
        };
    }

    /**
     * Translates {@code column IN (literal, ...)}.
     *
     * @param args column followed by the values
     * @return membership test, or empty
     */
    private static Optional<Expr> in(List<ResolvedExpression> args) {
        if (args.size() < 2 || !(args.get(0) instanceof FieldReferenceExpression field)) {
            return Optional.empty();
        }
        List<Expr.Literal> values = new ArrayList<>();
        for (ResolvedExpression arg : args.subList(1, args.size())) {
            if (!(arg instanceof ValueLiteralExpression value)) {
                return Optional.empty();
            }
            Optional<Expr.Literal> l = literal(field, value);
            if (l.isEmpty()) {
                return Optional.empty();
            }
            values.add(l.get());
        }
        return Optional.of(new Expr.In(column(field), values));
    }

    /**
     * Translates {@code column LIKE 'abc%' | '%abc' | '%abc%'} without an ESCAPE clause; patterns with other
     * wildcards or a backslash stay in Flink.
     *
     * @param args column and pattern
     * @return pattern match, or empty
     */
    private static Optional<Expr> like(List<ResolvedExpression> args) {
        if (args.size() != 2 || !(args.get(0) instanceof FieldReferenceExpression field)
                || !isType(field, LogicalTypeRoot.VARCHAR)
                || !(args.get(1) instanceof ValueLiteralExpression pattern)) {
            return Optional.empty();
        }
        Optional<String> p = pattern.getValueAs(String.class);
        if (p.isEmpty()) {
            return Optional.empty();
        }
        String s = p.get();
        boolean leading = s.startsWith("%");
        boolean trailing = s.length() > (leading ? 1 : 0) && s.endsWith("%");
        String body = s.substring(leading ? 1 : 0, s.length() - (trailing ? 1 : 0));
        if (!leading && !trailing || body.contains("%") || body.contains("_") || body.contains("\\")) {
            return Optional.empty();
        }
        Expr.LikeKind kind = leading && trailing ? Expr.LikeKind.CONTAINS
                : leading ? Expr.LikeKind.ENDS_WITH : Expr.LikeKind.STARTS_WITH;
        return Optional.of(new Expr.Like(column(field), kind, body));
    }

    /**
     * Converts a literal compared with a column when both have the same comparison semantics in Flink and on the
     * server.
     *
     * @param field column
     * @param value literal
     * @return literal, or empty (also for NULL: such comparisons are left to Flink)
     */
    private static Optional<Expr.Literal> literal(FieldReferenceExpression field, ValueLiteralExpression value) {
        if (value.isNull()) {
            return Optional.empty();
        }
        LogicalType columnType = field.getOutputDataType().getLogicalType();
        LogicalType valueType = value.getOutputDataType().getLogicalType();
        Object v = value.getValueAs(value.getOutputDataType().getConversionClass()).orElse(null);
        boolean matches = switch (columnType.getTypeRoot()) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> isExactNumeric(valueType)
                    && (v instanceof BigDecimal || v instanceof Long || v instanceof Integer || v instanceof Short
                    || v instanceof Byte);
            case VARCHAR -> isString(valueType) && v instanceof String;
            case BOOLEAN -> valueType.getTypeRoot() == LogicalTypeRoot.BOOLEAN && v instanceof Boolean;
            case DATE -> valueType.getTypeRoot() == LogicalTypeRoot.DATE && v instanceof LocalDate;
            // The server keeps microseconds: a literal with nanoseconds would be rounded there.
            case TIMESTAMP_WITHOUT_TIME_ZONE -> valueType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                    && v instanceof LocalDateTime t && t.getNano() % 1000 == 0;
            default -> false;
        };
        return matches ? Expr.Literal.of(v) : Optional.empty();
    }

    /**
     * Whether a literal type is an integer or DECIMAL.
     *
     * @param type literal type
     * @return true for exact numerics
     */
    private static boolean isExactNumeric(LogicalType type) {
        return switch (type.getTypeRoot()) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> true;
            default -> false;
        };
    }

    /**
     * Whether a literal type is a character string (Flink string literals are CHAR(n)).
     *
     * @param type literal type
     * @return true for CHAR and VARCHAR
     */
    private static boolean isString(LogicalType type) {
        return type.getTypeRoot() == LogicalTypeRoot.CHAR || type.getTypeRoot() == LogicalTypeRoot.VARCHAR;
    }

    /**
     * Whether a column has the given type root.
     *
     * @param field column
     * @param root type root
     * @return true when it matches
     */
    private static boolean isType(FieldReferenceExpression field, LogicalTypeRoot root) {
        return field.getOutputDataType().getLogicalType().getTypeRoot() == root;
    }

    /**
     * Column reference by the declared name.
     *
     * @param field column
     * @return server column
     */
    private static Expr.Column column(FieldReferenceExpression field) {
        return new Expr.Column(field.getName());
    }
}
