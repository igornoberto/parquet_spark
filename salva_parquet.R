
#' @name salva_parquet
#' @description Salva uma base no formato .parquet utilizando Spark
#' 
#' 
#' 
#' 


salva_parquet <- function(data, spark_connec, path){
  data_tbl <- copy_to(spark_connec, data, "data")
  spark_write_parquet(data_tbl, path)
}
