
#'  @name prepara_parquet
#'  @description Ajusta a base de entrada para ser convertida para parquet pelo Spark
#' 
#' Funcionalidade:
#'  Prepara a base para a transformação em parquet;
#'  Salva a base em parquet;
#'   
#' Entradas:
#'   Base Carregada/Base em disco
#'   
#'   
#'   
#'   @author Igor Noberto
#'   

library(stringr)
library(dplyr)
library(sparklyr)

prepara_parquet <- function(base, spark_connec = NULL, sep = ","){
  if(!exists("base") && !file.exists(base))
    stop("'base' needs to be a directory or an environment variable")
  if(exists("base")){
    #Trim, maiusculo
    names(base) <- names(base) %>% toupper() %>% str_trim()
    #Substitui caracteres por "_"
    names(base) <- names(base) %>% str_replace_all("\\.|\\s|\\*|\\%|\\!|\\@|\\&|\\(|\\)", "_")
  }
  if(file.exists(base)){
    if(is.null(spark_connec)){
      stop("spark_connec is missing!")
    }
    if(!isTRUE(spark_connection_is_open(spark_connec))){
      stop("There is no Spark connection open")
    }
    #Lê o arquivo (Por enquanto apenas csv)
    base_tbl <- spark_read_csv(sc = spark_connec, name =  "base_spark", path = base, delimiter = sep)
    #verifica leitura correta
    if("base_spark" %in% src_tbls(spark_connec)){
      
    }
  }
  
}


# ------ Corriqueiro: ------
sc <- spark_connect(master = "local")
spark_connection_is_open(sc)
iris_l <- copy_to(sc, iris, "iris")

b_names <- names(mtcars)

# ------ Testes: -----------
salva_parquet(mtcars, sc, "/Users/Igor/OneDrive/Documents/IPEA/IgorWD/mtcars_parquet")