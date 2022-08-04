package {daoPackageName};

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import {beanPackageName}.{beanName};

@Mapper
public interface {daoName} {{

    @Select("select * from {table_name} t where t.{idColumn} = #{{id}}")
    {beanName} getById({idType} id);

    @Delete("delete from {table_name} where {idColumn} = #{{id}}")
    int delete({idType} id);

    int update({beanName} {beanParamName});
    
    @Insert("insert into {table_name}({insert_columns}) values({insert_values})")
    int save({beanName} {beanParamName});
    
    int count(@Param("params") Map<String, Object> params);

    List<{beanName}> list(@Param("params") Map<String, Object> params, @Param("offset") Integer offset, @Param("limit") Integer limit);
}}
