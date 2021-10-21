package cz.tmsoft.springbatchmongo.repository;

import java.util.List;

import cz.tmsoft.springbatchmongo.domain.ImportData;
import cz.tmsoft.springbatchmongo.domain.StatusImport;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Repository
public interface ImportDao extends MongoRepository<ImportData, String> {

    public List<ImportData> findByStatus(StatusImport status);
}
