package cz.tmsoft.springbatchmongo.service;

import java.util.List;

import cz.tmsoft.springbatchmongo.domain.ImportData;
import cz.tmsoft.springbatchmongo.repository.ImportDao;
import org.springframework.stereotype.Service;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Service
public class ImportService {

    private ImportDao importDao;

    public ImportService(final ImportDao importDao) {
        this.importDao = importDao;
    }

    public void saveImport(final List<ImportData> imports) {
        for(ImportData imp : imports){
            importDao.save(imp);
        }
    }

    public ImportData getImport() {
        List<ImportData> all = importDao.findByStatus(null);
        return all.size() > 0 ? all.get(0) : null;
    }
}
