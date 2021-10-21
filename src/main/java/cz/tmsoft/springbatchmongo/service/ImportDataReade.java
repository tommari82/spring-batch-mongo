package cz.tmsoft.springbatchmongo.service;

import cz.tmsoft.springbatchmongo.domain.ImportData;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
public class ImportDataReade  implements ItemReader<ImportData> {

    @Autowired
    private  ImportService importService;

    @Override
    public ImportData read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return importService.getImport();
    }
}
