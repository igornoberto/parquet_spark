
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

prepara_parquet <- function(data){
  #Trim, maiusculo
  names(data) <- names(data) %>% toupper() %>% str_trim()
  #Substitui caracteres por "_"
  names(data) <- names(data) %>% str_replace_all("\\.|\\s|\\*|\\%|\\!|\\@|\\&|\\(|\\)", "_")
  
}


# ------ Corriqueiro: ------
sc <- spark_connect(master = "local")
spark_connection_is_open(sc)
iris_l <- copy_to(sc, iris, "iris")

b_names <- names(mtcars)

# ------ Testes: -----------
salva_parquet(mtcars, sc, "/Users/Igor/OneDrive/Documents/IPEA/IgorWD/mtcars_parquet")