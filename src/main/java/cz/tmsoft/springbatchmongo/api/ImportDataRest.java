package cz.tmsoft.springbatchmongo.api;

import lombok.Data;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Data
public class ImportDataRest {

    private String uuid;
    private String name;
    private String age;
    private String email;

}
