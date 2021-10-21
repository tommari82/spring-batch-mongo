package cz.tmsoft.springbatchmongo.service;

import java.util.List;

import cz.tmsoft.springbatchmongo.domain.ImportData;
import cz.tmsoft.springbatchmongo.domain.StatusImport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
public class ImportDataWrite implements ItemWriter<ImportData> {

    @Autowired
    private  ImportService importService;

    @Override
    public void write(final List<? extends ImportData> items) throws Exception {
        for(ImportData item  : items){
            item.setStatus(StatusImport.OK);

        }
        importService.saveImport((List<ImportData>) items);
    }
}
