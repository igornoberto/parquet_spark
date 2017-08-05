
#' @name salva_parquet
#' @description Salva uma base no formato .parquet utilizando Spark
#' 
#' 
#' 
#' 


library(stringr)
library(dplyr)
library(package)

salva_parquet <- function(base, spark_connec, path, sep = ","){
  if(!exists("base") && !file.exists(base)){
    stop("'base' needs to be a directory or an environment variable")
  }
  if(is.null(spark_connec)){
    stop("'spark_connec' is missing!")
  }
  if(!isTRUE(spark_connection_is_open(spark_connec))){
    stop("A Spark connection is required")
  }
  
  if(exists("base")){
    base_tbl <- copy_to(spark_connec, base, "base_spark")
    spark_write_parquet(base_tbl,path)
  }
  if(file.exists(base)){
    base_tbl <- spark_read_csv(sc = spark_connec, name =  "base_spark", path = base, delimiter = sep)
    if("base_spark" %in% src_tbls(spark_connec)){
      spark_write_parquet(base_tbl,path)
    }
  }
}
