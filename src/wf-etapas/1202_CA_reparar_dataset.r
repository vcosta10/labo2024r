#!/usr/bin/env Rscript

# Workflow  Catastrophe Analysis

# inputs
#  * dataset
# output  
#   dataset :
#     misma cantidad de registros
#     misma cantidad de atributos
#     valores modificados para las  < variables, mes > que habian sido dañadas con un  0

# limpio la memoria
rm(list = ls(all.names = TRUE)) # remove all objects
gc(full = TRUE) # garbage collection

require("data.table")
require("yaml")

#cargo la libreria
# args <- c( "~/labo2024r" )
args <- commandArgs(trailingOnly=TRUE)
source( paste0( args[1] , "/src/lib/action_lib.r" ) )

# Instalación de paquete (en caso de que sea necesario)
if(!("mice" %in% installed.packages())) install.packages("mice", repos = "http://cran.us.r-project.org")
library(mice)
#------------------------------------------------------------------------------

CorregirCampoMes <- function(pcampo, pmeses) {
  
  tbl <- dataset[, 
                 list(
                   "foto_mes" = get(envg$PARAM$dataset_metadata$periodo),
                   "clase_actual" = get(envg$PARAM$dataset_metadata$clase),
                   "clase_lead1" = shift(get(envg$PARAM$dataset_metadata$clase), 1, type = "lead"),
                   "lag1" = shift(get(pcampo), 1, type = "lag"),
                   "lag2" = shift(get(pcampo), 2, type = "lag"),
                   "lead1" = shift(get(pcampo), 1, type = "lead"),
                   "lead2" = shift(get(pcampo), 2, type = "lead")
                 ),
                 by = eval(envg$PARAM$dataset_metadata$entity_id)
  ]
  
  tbl[, lead1 := ifelse(clase_actual == "BAJA+2", NA, lead1)] # Ignorar lead1 para los BAJA+2
  tbl[, lead2 := ifelse(clase_lead1  == "BAJA+2", NA, lead2)] # Idem para los que seran BAJA+2 el mes siguiente
  
  tbl[, lag1  := ifelse((foto_mes-1) %in% pmeses,  lag2,  lag1)] # Tomar un lag mas atras si el mes anterior esta roto
  tbl[, lead1 := ifelse((foto_mes+1) %in% pmeses, lead2, lead1)] # Tomar un lead mas adelante si el mes proximo esta roto
  
  tbl[, paste0(envg$PARAM$dataset_metadata$entity_id) := NULL]
  tbl[, foto_mes := NULL]
  tbl[, clase_actual := NULL]
  tbl[, clase_lead1 := NULL]
  tbl[, lag2 := NULL]
  tbl[, lead2 := NULL]
  
  tbl[, promedio := rowMeans(tbl, na.rm = TRUE)]
  
  tbl[, promedio := ifelse(is.nan(promedio), NA, promedio)] # reemplazar NaN por NA
  
  dataset[
    ,
    paste0(pcampo) := ifelse(
      eval(envg$PARAM$dataset_metadata$periodo) %in% pmeses,
      tbl$promedio,
      get(pcampo)
    )
  ]
}
#------------------------------------------------------------------------------

CorregirCampoMes_MICE <- function(pcampo, pmeses) {

  meth <- rep("", ncol(dataset))
  names(meth) <- colnames(dataset)
  meth[names(meth) == pcampo] <- "sample"
  imputacion <- mice(dataset, method = meth, maxit = 5, m = 1, seed = 7)
  tbl <- mice::complete(dataset)
  
  dataset[, paste0(pcampo) := ifelse(foto_mes %in% pmeses, tbl[, get(pcampo)], get(pcampo))]
}
#------------------------------------------------------------------------------


# reemplaza cada variable ROTA  (variable, foto_mes)
#  con el promedio entre  ( mes_anterior, mes_posterior )

Corregir_EstadisticaClasica <- function(dataset) {
  cat( "inicio Corregir_EstadisticaClasica()\n")

  CorregirCampoMes("active_quarter", c(202006)) # 1
  CorregirCampoMes("internet", c(202006)) # 2

  CorregirCampoMes("mrentabilidad", c(201905, 201910, 202006)) # 3
  CorregirCampoMes("mrentabilidad_annual", c(201905, 201910, 202006)) # 4

  CorregirCampoMes("mcomisiones", c(201905, 201910, 202006)) # 5

  CorregirCampoMes("mactivos_margen", c(201905, 201910, 202006)) # 6
  CorregirCampoMes("mpasivos_margen", c(201905, 201910, 202006)) # 7

  CorregirCampoMes("mcuentas_saldo", c(202006)) # 8
  
  CorregirCampoMes("ctarjeta_debito_transacciones", c(202006)) # 9

  CorregirCampoMes("mautoservicio", c(202006)) # 10

  CorregirCampoMes("ctarjeta_visa_transacciones", c(202006)) # 11
  CorregirCampoMes("mtarjeta_visa_consumo", c(202006)) # 12

  CorregirCampoMes("ctarjeta_master_transacciones", c(202006)) # 13
  CorregirCampoMes("mtarjeta_master_consumo", c(202006)) # 14

  CorregirCampoMes("ctarjeta_visa_debitos_automaticos", c(201904)) # 15
  CorregirCampoMes("mttarjeta_visa_debitos_automaticos", c(201904)) # 16

  CorregirCampoMes("ccajeros_propios_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 17
  CorregirCampoMes("mcajeros_propios_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 18
  CorregirCampoMes("ctarjeta_visa_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 19
  CorregirCampoMes("mtarjeta_visa_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 20
  CorregirCampoMes("ctarjeta_master_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 21
  CorregirCampoMes("mtarjeta_master_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 22

  CorregirCampoMes("ccomisiones_otras", c(201905, 201910, 202006)) # 23
  CorregirCampoMes("mcomisiones_otras", c(201905, 201910, 202006)) # 24

  CorregirCampoMes("cextraccion_autoservicio", c(202006)) # 25
  CorregirCampoMes("mextraccion_autoservicio", c(202006)) # 26

  CorregirCampoMes("ccheques_depositados", c(202006)) # 27
  CorregirCampoMes("mcheques_depositados", c(202006)) # 28
  CorregirCampoMes("ccheques_emitidos", c(202006)) # 29
  CorregirCampoMes("mcheques_emitidos", c(202006)) # 30
  CorregirCampoMes("ccheques_depositados_rechazados", c(202006)) # 31
  CorregirCampoMes("mcheques_depositados_rechazados", c(202006)) # 32
  CorregirCampoMes("ccheques_emitidos_rechazados", c(202006)) # 33
  CorregirCampoMes("mcheques_emitidos_rechazados", c(202006)) # 34

  CorregirCampoMes("tcallcenter", c(202006)) # 35
  CorregirCampoMes("ccallcenter_transacciones", c(202006)) # 36

  CorregirCampoMes("thomebanking", c(202006)) # 37
  CorregirCampoMes("chomebanking_transacciones", c(201910, 202006)) # 38

  CorregirCampoMes("ccajas_transacciones", c(202006)) # 39
  CorregirCampoMes("ccajas_consultas", c(202006)) # 40

  CorregirCampoMes("ccajas_depositos", c(202006, 202105)) # 41

  CorregirCampoMes("ccajas_extracciones", c(202006)) # 41
  CorregirCampoMes("ccajas_otras", c(202006)) # 43

  CorregirCampoMes("catm_trx", c(202006)) # 44
  CorregirCampoMes("matm", c(202006)) # 45
  CorregirCampoMes("catm_trx_other", c(202006)) # 46
  CorregirCampoMes("matm_other", c(202006)) # 47

  cat( "fin Corregir_EstadisticaClasica()\n")
}
#------------------------------------------------------------------------------

Corregir_MachineLearning <- function(dataset) {
  gc()
  cat( "inicio Corregir_MachineLearning()\n")
  # acomodo los errores del dataset

  dataset[foto_mes %in% c(202006), active_quarter := NA] # 1
  dataset[foto_mes %in% c(202006), internet := NA] # 2

  dataset[foto_mes %in% c(201905, 201910, 202006), mrentabilidad := NA] # 3
  dataset[foto_mes %in% c(201905, 201910, 202006), mrentabilidad_annual := NA] # 4

  dataset[foto_mes %in% c(201905, 201910, 202006), mcomisiones := NA] # 5

  dataset[foto_mes %in% c(201905, 201910, 202006), mactivos_margen := NA] # 6
  dataset[foto_mes %in% c(201905, 201910, 202006), mpasivos_margen := NA] # 7

  dataset[foto_mes %in% c(202006), mcuentas_saldo := NA] # 8

  dataset[foto_mes %in% c(202006), ctarjeta_debito_transacciones := NA] # 9

  dataset[foto_mes %in% c(202006), mautoservicio := NA] # 10

  dataset[foto_mes %in% c(202006), ctarjeta_visa_transacciones := NA] # 11
  dataset[foto_mes %in% c(202006), mtarjeta_visa_consumo := NA] # 12

  dataset[foto_mes %in% c(202006), ctarjeta_master_transacciones := NA] # 13
  dataset[foto_mes %in% c(202006), mtarjeta_master_consumo := NA] # 14

  dataset[foto_mes %in% c(201904), ctarjeta_visa_debitos_automaticos := NA] # 15
  dataset[foto_mes %in% c(201904), mttarjeta_visa_debitos_automaticos := NA] # 16

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), ccajeros_propios_descuentos := NA] # 17
  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), mcajeros_propios_descuentos := NA] # 18
  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), ctarjeta_visa_descuentos := NA] # 19
  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), mtarjeta_visa_descuentos := NA] # 20
  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), ctarjeta_master_descuentos := NA] # 21
  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), mtarjeta_master_descuentos := NA] # 22


  dataset[foto_mes %in% c(201905, 201910, 202006), ccomisiones_otras:= NA] # 23
  dataset[foto_mes %in% c(201905, 201910, 202006), mcomisiones_otras := NA] # 24

  dataset[foto_mes %in% c(202006), cextraccion_autoservicio := NA] # 25
  dataset[foto_mes %in% c(202006), mextraccion_autoservicio := NA] # 26

  dataset[foto_mes %in% c(202006), ccheques_depositados := NA] # 27
  dataset[foto_mes %in% c(202006), mcheques_depositados := NA] # 28
  dataset[foto_mes %in% c(202006), ccheques_emitidos := NA] # 29
  dataset[foto_mes %in% c(202006), mcheques_emitidos := NA] # 30
  dataset[foto_mes %in% c(202006), ccheques_depositados_rechazados := NA] # 31
  dataset[foto_mes %in% c(202006), mcheques_depositados_rechazados := NA] # 32
  dataset[foto_mes %in% c(202006), ccheques_emitidos_rechazados := NA] # 33
  dataset[foto_mes %in% c(202006), mcheques_emitidos_rechazados := NA] # 34

  dataset[foto_mes %in% c(202006), tcallcenter := NA] # 35
  dataset[foto_mes %in% c(202006), ccallcenter_transacciones := NA] # 36

  dataset[foto_mes %in% c(202006), thomebanking := NA] # 37
  dataset[foto_mes %in% c(201910, 202006), chomebanking_transacciones := NA] # 38

  dataset[foto_mes %in% c(202006), ccajas_transacciones := NA] # 39
  dataset[foto_mes %in% c(202006), ccajas_consultas := NA] # 40

  dataset[foto_mes %in% c(202006, 202105), ccajas_depositos := NA] # 41

  dataset[foto_mes %in% c(202006), ccajas_extracciones := NA] # 42
  dataset[foto_mes %in% c(202006), ccajas_otras := NA] # 43

  dataset[foto_mes %in% c(202006), catm_trx := NA] # 44
  dataset[foto_mes %in% c(202006), matm := NA] # 45
  dataset[foto_mes %in% c(202006), catm_trx_other := NA] # 46
  dataset[foto_mes %in% c(202006), matm_other := NA] # 47

  cat( "fin Corregir_MachineLearning()\n")
}
#------------------------------------------------------------------------------

Corregir_MLEC <- function(dataset) {
  gc()
  cat( "inicio Corregir_MLEC()\n")
  
  Corregir_MachineLearning(dataset)
  
  Corregir_EstadisticaClasica(dataset)
  
  cat( "fin Corregir_MLEC()\n")
}
#------------------------------------------------------------------------------

Corregir_MICE <- function(dataset) {
  
  # se aplica previo la asignacion de NA por como funciona mice
  
  dataset[foto_mes %in% c(202006), active_quarter := NA] # 1
  CorregirCampoMes_MICE("active_quarter", c(202006)) # 1
  
  dataset[foto_mes %in% c(202006), internet := NA] # 2
  CorregirCampoMes_MICE("internet", c(202006)) # 2

  dataset[foto_mes %in% c(201905, 201910, 202006), mrentabilidad := NA] # 3
  CorregirCampoMes_MICE("mrentabilidad", c(201905, 201910, 202006)) # 3

  dataset[foto_mes %in% c(201905, 201910, 202006), mrentabilidad_annual := NA] # 4
  CorregirCampoMes_MICE("mrentabilidad_annual", c(201905, 201910, 202006)) # 4

  dataset[foto_mes %in% c(201905, 201910, 202006), mcomisiones := NA] # 5
  CorregirCampoMes_MICE("mcomisiones", c(201905, 201910, 202006)) # 5

  dataset[foto_mes %in% c(201905, 201910, 202006), mactivos_margen := NA] # 6
  CorregirCampoMes_MICE("mactivos_margen", c(201905, 201910, 202006)) # 6

  dataset[foto_mes %in% c(201905, 201910, 202006), mpasivos_margen := NA] # 7
  CorregirCampoMes_MICE("mpasivos_margen", c(201905, 201910, 202006)) # 7

  dataset[foto_mes %in% c(202006), mcuentas_saldo := NA] # 8
  CorregirCampoMes_MICE("mcuentas_saldo", c(202006)) # 8

  dataset[foto_mes %in% c(202006), ctarjeta_debito_transacciones := NA] # 9
  CorregirCampoMes_MICE("ctarjeta_debito_transacciones", c(202006)) # 9

  dataset[foto_mes %in% c(202006), mautoservicio := NA] # 10
  CorregirCampoMes_MICE("mautoservicio", c(202006)) # 10

  dataset[foto_mes %in% c(202006), ctarjeta_visa_transacciones := NA] # 11
  CorregirCampoMes_MICE("ctarjeta_visa_transacciones", c(202006)) # 11

  dataset[foto_mes %in% c(202006), mtarjeta_visa_consumo := NA] # 12
  CorregirCampoMes_MICE("mtarjeta_visa_consumo", c(202006)) # 12

  dataset[foto_mes %in% c(202006), ctarjeta_master_transacciones := NA] # 13
  CorregirCampoMes_MICE("ctarjeta_master_transacciones", c(202006)) # 13

  dataset[foto_mes %in% c(202006), mtarjeta_master_consumo := NA] # 14
  CorregirCampoMes_MICE("mtarjeta_master_consumo", c(202006)) # 14

  dataset[foto_mes %in% c(201904), ctarjeta_visa_debitos_automaticos := NA] # 15
  CorregirCampoMes_MICE("ctarjeta_visa_debitos_automaticos", c(201904)) # 15

  dataset[foto_mes %in% c(201904), mttarjeta_visa_debitos_automaticos := NA] # 16
  CorregirCampoMes_MICE("mttarjeta_visa_debitos_automaticos", c(201904)) # 16

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), ccajeros_propios_descuentos := NA] # 17
  CorregirCampoMes_MICE("ccajeros_propios_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 17

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), mcajeros_propios_descuentos := NA] # 18
  CorregirCampoMes_MICE("mcajeros_propios_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 18

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), ctarjeta_visa_descuentos := NA] # 19
  CorregirCampoMes_MICE("ctarjeta_visa_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 19

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), mtarjeta_visa_descuentos := NA] # 20
  CorregirCampoMes_MICE("mtarjeta_visa_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 20

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), ctarjeta_master_descuentos := NA] # 21
  CorregirCampoMes_MICE("ctarjeta_master_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 21

  dataset[foto_mes %in% c(201910, 202002, 202006, 202009, 202010, 202102), mtarjeta_master_descuentos := NA] # 22
  CorregirCampoMes_MICE("mtarjeta_master_descuentos", c(201910, 202002, 202006, 202009, 202010, 202102)) # 22

  dataset[foto_mes %in% c(201905, 201910, 202006), ccomisiones_otras := NA] # 23
  CorregirCampoMes_MICE("ccomisiones_otras", c(201905, 201910, 202006)) # 23

  dataset[foto_mes %in% c(201905, 201910, 202006), mcomisiones_otras := NA] # 24
  CorregirCampoMes_MICE("mcomisiones_otras", c(201905, 201910, 202006)) # 24

  dataset[foto_mes %in% c(202006), cextraccion_autoservicio := NA] # 25
  CorregirCampoMes_MICE("cextraccion_autoservicio", c(202006)) # 25

  dataset[foto_mes %in% c(202006), mextraccion_autoservicio := NA] # 26
  CorregirCampoMes_MICE("mextraccion_autoservicio", c(202006)) # 26

  dataset[foto_mes %in% c(202006), ccheques_depositados := NA] # 27
  CorregirCampoMes_MICE("ccheques_depositados", c(202006)) # 27

  dataset[foto_mes %in% c(202006), mcheques_depositados := NA] # 28
  CorregirCampoMes_MICE("mcheques_depositados", c(202006)) # 28

  dataset[foto_mes %in% c(202006), ccheques_emitidos := NA] # 29
  CorregirCampoMes_MICE("ccheques_emitidos", c(202006)) # 29

  dataset[foto_mes %in% c(202006), mcheques_emitidos := NA] # 30
  CorregirCampoMes_MICE("mcheques_emitidos", c(202006)) # 30

  dataset[foto_mes %in% c(202006), ccheques_depositados_rechazados := NA] # 31
  CorregirCampoMes_MICE("ccheques_depositados_rechazados", c(202006)) # 31

  dataset[foto_mes %in% c(202006), mcheques_depositados_rechazados := NA] # 32
  CorregirCampoMes_MICE("mcheques_depositados_rechazados", c(202006)) # 32

  dataset[foto_mes %in% c(202006), ccheques_emitidos_rechazados := NA] # 33
  CorregirCampoMes_MICE("ccheques_emitidos_rechazados", c(202006)) # 33

  dataset[foto_mes %in% c(202006), mcheques_emitidos_rechazados := NA] # 34
  CorregirCampoMes_MICE("mcheques_emitidos_rechazados", c(202006)) # 34

  dataset[foto_mes %in% c(202006), tcallcenter := NA] # 35
  CorregirCampoMes_MICE("tcallcenter", c(202006)) # 35

  dataset[foto_mes %in% c(202006), ccallcenter_transacciones := NA] # 36
  CorregirCampoMes_MICE("ccallcenter_transacciones", c(202006)) # 36

  dataset[foto_mes %in% c(202006), thomebanking := NA] # 37
  CorregirCampoMes_MICE("thomebanking", c(202006)) # 37

  dataset[foto_mes %in% c(201910, 202006), chomebanking_transacciones := NA] # 38
  CorregirCampoMes_MICE("chomebanking_transacciones", c(201910, 202006)) # 38

  dataset[foto_mes %in% c(202006), ccajas_transacciones := NA] # 39
  CorregirCampoMes_MICE("ccajas_transacciones", c(202006)) # 39

  dataset[foto_mes %in% c(202006), ccajas_consultas := NA] # 40
  CorregirCampoMes_MICE("ccajas_consultas", c(202006)) # 40

  dataset[foto_mes %in% c(202006, 202105), ccajas_depositos := NA] # 41
  CorregirCampoMes_MICE("ccajas_depositos", c(202006, 202105)) # 41

  dataset[foto_mes %in% c(202006), ccajas_extracciones := NA] # 42
  CorregirCampoMes_MICE("ccajas_extracciones", c(202006)) # 42

  dataset[foto_mes %in% c(202006), ccajas_otras := NA] # 43
  CorregirCampoMes_MICE("ccajas_otras", c(202006)) # 43

  dataset[foto_mes %in% c(202006), catm_trx := NA] # 44
  CorregirCampoMes_MICE("catm_trx", c(202006)) # 44

  dataset[foto_mes %in% c(202006), matm := NA] # 45
  CorregirCampoMes_MICE("matm", c(202006)) # 45

  dataset[foto_mes %in% c(202006), catm_trx_other := NA] # 46
  CorregirCampoMes_MICE("catm_trx_other", c(202006)) # 46

  dataset[foto_mes %in% c(202006), matm_other := NA] # 47
  CorregirCampoMes_MICE("matm_other", c(202006)) # 47
  
  cat("fin Corregir_MICE()\n")
}

#------------------------------------------------------------------------------
# Aqui empieza el programa
cat( "1202_CA_reparar_dataset.r  START\n")
action_inicializar() 

# cargo el dataset
envg$PARAM$dataset <- paste0( "./", envg$PARAM$input, "/dataset.csv.gz" )
envg$PARAM$dataset_metadata <- read_yaml( paste0( "./", envg$PARAM$input, "/dataset_metadata.yml" ) )

cat( "lectura del dataset\n")
action_verificar_archivo( envg$PARAM$dataset )
cat( "Iniciando lectura del dataset\n" )
dataset <- fread(envg$PARAM$dataset)
cat( "Finalizada lectura del dataset\n" )

# tmobile_app   se daño a partir de 202010
dataset[, tmobile_app := NULL]

# cmobile_app_trx   se daño a partir de 202010, ya que empieza a valer 1
dataset[, cmobile_app_trx := NULL]

GrabarOutput()

# ordeno dataset
setorderv(dataset, envg$PARAM$dataset_metadata$primarykey)

# corrijo los  < foto_mes, campo >  que fueron pisados con cero
switch( envg$PARAM$metodo,
  "MachineLearning"     = Corregir_MachineLearning(dataset), # 001
  "EstadisticaClasica"  = Corregir_EstadisticaClasica(dataset), # 003
  "MICE"                = Corregir_MICE(dataset), # 005
  "MLEC"                = Corregir_MLEC(dataset), # 004
  "Ninguno"             = cat("No se aplica ninguna correccion.\n"), # 002
)

#------------------------------------------------------------------------------
# grabo el dataset
cat( "grabado del dataset\n")
cat( "Iniciando grabado del dataset\n" )
fwrite(dataset,
  file = "dataset.csv.gz",
  logical01 = TRUE,
  sep = ","
)
cat( "Finalizado grabado del dataset\n" )

# copia la metadata sin modificar
cat( "grabado metadata\n")
write_yaml( envg$PARAM$dataset_metadata, 
  file="dataset_metadata.yml" )

#------------------------------------------------------------------------------

# guardo los campos que tiene el dataset
tb_campos <- as.data.table(list(
  "pos" = 1:ncol(dataset),
  "campo" = names(sapply(dataset, class)),
  "tipo" = sapply(dataset, class),
  "nulos" = sapply(dataset, function(x) {
    sum(is.na(x))
  }),
  "ceros" = sapply(dataset, function(x) {
    sum(x == 0, na.rm = TRUE)
  })
))

fwrite(tb_campos,
  file = "dataset.campos.txt",
  sep = "\t"
)

#------------------------------------------------------------------------------
cat( "Fin del programa\n")

envg$OUTPUT$dataset$ncol <- ncol(dataset)
envg$OUTPUT$dataset$nrow <- nrow(dataset)
envg$OUTPUT$time$end <- format(Sys.time(), "%Y%m%d %H%M%S")
GrabarOutput()

#------------------------------------------------------------------------------
# finalizo la corrida
#  archivos tiene a los files que debo verificar existen para no abortar

action_finalizar( archivos = c("dataset.csv.gz","dataset_metadata.yml")) 
cat( "1202_CA_reparar_dataset.r  END\n")
