package io.wiklandia.avro;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

@Slf4j
@Service
public class ConvertService {

    private final DateTimeFormatter formatter;

    public ConvertService(BatchProperties properties) {
        this.formatter = DateTimeFormatter.ofPattern(properties.getDatePattern());
    }

    private List<Converter> converters() {
        return List.of(
                nullConverter(),
                decimalConverter(),
                dateConverter(),
                intConverter()
        );
    }

    public Object convert(Schema fieldSchema, String value) {
        Schema properSchema = getProperSchema(fieldSchema);
        return converters()
                .stream()
                .filter(converter -> converter.valid.test(properSchema, value))
                .map(converter -> converter.convert.apply(properSchema, value))
                .findFirst()
                .orElse(value);
    }

    Converter nullConverter() {
        return Converter.of(
                (schema, s) -> s == null,
                (schema, s) -> s);
    }

    Converter decimalConverter() {
        return Converter.of(
                (schema, s) -> schema.getLogicalType() instanceof LogicalTypes.Decimal,
                (schema, s) -> {
                    LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) schema.getLogicalType();
                    BigDecimal bigDecimal = new BigDecimal(s).setScale(logicalType.getScale(), RoundingMode.UNNECESSARY);
                    return new Conversions.DecimalConversion().toBytes(bigDecimal, schema, schema.getLogicalType());
                });
    }

    Converter dateConverter() {
        return Converter.of(
                (schema, s) -> schema.getLogicalType() instanceof LogicalTypes.Date,
                (schema, s) -> new TimeConversions.DateConversion()
                        .toInt(LocalDate.parse(s, formatter), schema, schema.getLogicalType()));
    }

    Converter intConverter() {
        return Converter.of(
                (schema, s) -> schema.getType() == Schema.Type.INT,
                (schema, s) -> new BigDecimal(s).intValueExact());
    }

    Schema getProperSchema(Schema fieldSchema) {
        Schema.Type type = fieldSchema.getType();
        if (type == Schema.Type.UNION) {
            return fieldSchema.getFields()
                    .stream()
                    .map(Schema.Field::schema)
                    .map(this::getProperSchema)
                    .findFirst()
                    .orElseThrow();
        } else if (type == Schema.Type.NULL) {
            return null;
        } else {
            return fieldSchema;
        }
    }

    @Data
    @AllArgsConstructor(staticName = "of")
    static class Converter {
        private BiPredicate<Schema, String> valid;
        private BiFunction<Schema, String, Object> convert;
    }

}
