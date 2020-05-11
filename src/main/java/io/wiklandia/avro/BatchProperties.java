package io.wiklandia.avro;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

@ConfigurationProperties("app")
@Component
@Data
public class BatchProperties {
    private Resource schemaFile;
    private Resource inputFile;
}
