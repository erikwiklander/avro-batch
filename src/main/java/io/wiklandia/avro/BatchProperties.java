package io.wiklandia.avro;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;

@ConfigurationProperties("app")
@Component
@Data
public class BatchProperties {
    private Resource schemaFile;
}
