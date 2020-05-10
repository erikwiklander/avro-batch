package io.wiklandia.avro;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.avro.AvroItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.io.File;

@EnableBatchProcessing
@Configuration
@AllArgsConstructor
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BatchProperties properties;
    private final ConvertService convertService;

    @Bean
    public FlatFileItemReader<String[]> reader() {
        return new FlatFileItemReaderBuilder<String[]>()
                .name("recordItemReader")
                .resource(new ClassPathResource("sample.csv"))
                .lineMapper((s, i) -> s.split(","))
                .build();
    }

    @SneakyThrows
    @Bean
    public ItemProcessor<String[], GenericRecord> processor() {
        return new ItemProcessor<>() {
            Schema schema = new Schema.Parser().parse(properties.getSchemaFile().getInputStream());
            @Override
            public GenericRecord process(String[] strings) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                for (Schema.Field field : schema.getFields()) {
                    Object value = convertService.convert(field.schema(), strings[field.pos()]);
                    builder.set(field, value);
                }
                return builder.build();
            }
        };
    }

    @SneakyThrows
    @Bean
    public ItemWriter<GenericRecord> writer() {
        return new AvroItemWriter<>(new FileSystemResource(new File("out.avro")), properties.getSchemaFile(), GenericRecord.class);
    }

    @Bean
    public Job convertToAvro(Step step) {
        return jobBuilderFactory.get("convertToAvro")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step() {
        return stepBuilderFactory.get("step")
                .<String[], GenericRecord> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }


}
