package com.ccc.casualty.avro;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.avro.AvroItemWriter;
import org.springframework.batch.item.excel.poi.PoiItemReader;
import org.springframework.batch.item.excel.support.rowset.RowSet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.ArrayFieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.io.File;

@Slf4j
@EnableBatchProcessing
@Configuration
@AllArgsConstructor
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BatchProperties properties;
    private final ConvertService convertService;

    @SneakyThrows
    @Bean
    public ItemReader<String[]> reader() {
        boolean isExcel = properties.getInputFile().getFile().getName().toLowerCase().endsWith("xlsx");
        return isExcel
                ? poiReader()
                : flatFileReader();
    }

    FlatFileItemReader<String[]> flatFileReader() {
        return new FlatFileItemReaderBuilder<String[]>()
                .name("textItemReader")
                .resource(properties.getInputFile())
                .lineTokenizer(new DelimitedLineTokenizer(properties.getDelimiter()))
                .fieldSetMapper(new ArrayFieldSetMapper())
                .linesToSkip(properties.getLinesToSkip())
                .build();
    }

    PoiItemReader<String[]> poiReader() {
        PoiItemReader<String[]> reader = new PoiItemReader<>();
        reader.setResource(properties.getInputFile());
        reader.setRowMapper(RowSet::getCurrentRow);
        reader.setName("excelItemReader");
        reader.setLinesToSkip(properties.getLinesToSkip());
        return reader;
    }

    @SneakyThrows
    @Bean
    public ItemProcessor<String[], GenericRecord> processor() {
        return new ItemProcessor<>() {
            final Schema schema = new Schema.Parser().parse(properties.getSchemaFile().getInputStream());

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
        return new AvroItemWriter<>(
                new FileSystemResource(
                        new File(properties.getOutputFile())), properties.getSchemaFile(), GenericRecord.class);
    }

    @Bean
    public Job textToAvro(Step step) {
        return jobBuilderFactory.get("textToAvro")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step() {
        return stepBuilderFactory.get("step")
                .<String[], GenericRecord>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }


}
