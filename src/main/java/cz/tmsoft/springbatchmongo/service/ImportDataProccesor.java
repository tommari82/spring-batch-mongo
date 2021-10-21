package cz.tmsoft.springbatchmongo.service;

import cz.tmsoft.springbatchmongo.domain.ImportData;
import org.springframework.batch.item.ItemProcessor;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
public class ImportDataProccesor  implements ItemProcessor<ImportData, ImportData> {
    @Override
    public ImportData process(final ImportData item) throws Exception {
        return item;
    }
}
