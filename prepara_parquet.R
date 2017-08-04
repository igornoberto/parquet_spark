
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
  names(data) <- names(data) %>% toupper() %>% str_trim()
  names(data) <- str_trim(toupper(names(data)))
  
  
  names(plant) <- mutate_each(names(plant), funs(toupper))
  names(plant) <- toupper(names(plant))
}


# ------ Corriqueiro: ------
install.packages("dplyr")
library(sparklyr)
sc <- spark_connect(master = "local")
spark_connection_is_open(sc)
iris_l <- copy_to(sc, iris, "iris")

b_names <- names(mtcars)

# ------ Testes: -----------
salva_parquet(mtcars, sc, "/Users/Igor/OneDrive/Documents/IPEA/IgorWD/mtcars_parquet")