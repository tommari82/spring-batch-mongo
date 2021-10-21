package cz.tmsoft.springbatchmongo.domain;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Document
@Data
public class ImportData {
    @Id
    private String uuid;
    private String name;
    private String age;
    private String email;
    private StatusImport status;
}
