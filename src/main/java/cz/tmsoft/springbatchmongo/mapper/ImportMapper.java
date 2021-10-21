package cz.tmsoft.springbatchmongo.mapper;

import java.util.List;

import cz.tmsoft.springbatchmongo.api.ImportDataRest;
import cz.tmsoft.springbatchmongo.domain.ImportData;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Mapper(unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface ImportMapper {
    ImportMapper INSTANCE = Mappers.getMapper(ImportMapper.class);

    List<ImportData> map(List<ImportDataRest> request);

    ImportData mapp(ImportDataRest importDataRest);
}
