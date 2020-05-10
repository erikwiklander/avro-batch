package io.wiklandia.avro;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

@Slf4j
@Service
public class ConvertService {

    private List<Converter> converters() {
        return List.of(
                Converter.of(
                        (schema, s) -> s == null,
                        (schema, s) -> s
                ),
                Converter.of(
                        (schema, s) -> schema.getLogicalType() instanceof LogicalTypes.Decimal,
                        (schema, s) -> {
                            LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) schema.getLogicalType();
                            BigDecimal bigDecimal = new BigDecimal(s).setScale(logicalType.getScale(), RoundingMode.UNNECESSARY);
                            return new Conversions.DecimalConversion().toBytes(bigDecimal, schema, schema.getLogicalType());
                        }
                ),
                Converter.of(
                        (schema, s) -> schema.getLogicalType() instanceof LogicalTypes.Date,
                        (schema, s) -> new TimeConversions.DateConversion().toInt(LocalDate.parse(s), schema, schema.getLogicalType())
                ),
                Converter.of(
                        (schema, s) -> schema.getType() == Schema.Type.INT,
                        (schema, s) -> Integer.valueOf(s)
                ),
                Converter.of(
                        (schema, s) -> true,
                        (schema, s) -> s
                )
        );
    }


    public Object convert(Schema fieldSchema, String value) {
        Schema properSchema = getProperSchema(fieldSchema);
        log.info("ok {}", properSchema);
        return converters()
                .stream()
                .filter(converter -> converter.valid.test(properSchema, value))
                .map(converter -> converter.convert.apply(properSchema, value))
                .findFirst()
                .orElseThrow();
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
        }
        if (type == Schema.Type.NULL) {
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
