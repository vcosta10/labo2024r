require("rlang")
require("rlist")
require("yaml")
require("mlflow")
require("digest")


if( !exists("envg") ) envg <- env()  # global environment 

#------------------------------------------------------------------------------
#inicializo el ambiente de mlflow

mlog_mlflow_iniciar  <- function()
{
  #leo uri, usuario y password
  envg$mlog$mlflow$conn <- read_yaml(  paste0("/home/", envg$mlog$usuario, "/install/mlflow.yml" ) )
  envg$mlog$mlflow$conn$tracking_uri <- gsub( "\"", "", envg$mlog$mlflow$conn$tracking_uri )

  # seteo variables de entorno que necesita mlflow CLI
  Sys.setenv( MLFLOW_TRACKING_USERNAME= envg$mlog$mlflow$conn$tracking_username )
  Sys.setenv( MLFLOW_TRACKING_PASSWORD= envg$mlog$mlflow$conn$tracking_password )
  res <- mlflow_set_tracking_uri( envg$mlog$mlflow$conn$tracking_uri )

  Sys.setenv( PATH=paste0( "/home/", envg$mlog$usuario, "/.venv/bin:",
                           Sys.getenv("PATH")) )

  # mas seteo variables de entorno que necesita mlflow CLI
  Sys.setenv(MLFLOW_BIN= Sys.which("mlflow") )
  Sys.setenv(MLFLOW_PYTHON_BIN= Sys.which("python3") )
  Sys.setenv(MLFLOW_TRACKING_URI= envg$mlog$mlflow$conn$tracking_uri, intern= TRUE )
}
#------------------------------------------------------------------------------
# log de una list()

mlog_log_mlflow_reg  <- function( preg, prev="", onlymetrics=FALSE )
{
  prefijo <- ifelse( prev=="", "", paste0(prev,".") )

  for( campo in names(preg) )
  {
    campo_mostrar <- paste0(prefijo, campo)
    tipo <- typeof( preg[[ campo ]] )

    if( tipo %in% c("double","integer") )
         mlflow_log_metric( campo_mostrar, preg[[ campo ]] )

    if( tipo %in% c("logical") )
      mlflow_log_metric( campo_mostrar, as.integer(preg[[ campo ]]) )

    if( tipo %in% c("character", "symbol") & !onlymetrics )
      mlflow_log_param( campo_mostrar, preg[[ campo ]] )

    if( tipo %in% c("list") )
    {
      nuevo_prefijo <- paste0( prefijo, campo )
      mlog_log_mlflow_reg( preg[[ campo ]], nuevo_prefijo, onlymetrics )
    }
  }
 
}
#------------------------------------------------------------------------------
# registro "linea"  en MLFlow

mlog_log_mlflow  <- function( reg, archivo, t0, parentreplicate=FALSE )
{
  #Inicio mlflow de ser necesario
  if( ! envg$mlog$mlflow$iniciado )
  {
    mlog_mlflow_iniciar()
    envg$mlog$mlflow$iniciado <- TRUE
  }

  tarch <- envg$mlog$larch$archivos[[ archivo ]]

  # seteo el experimento, lo crea si no existe
  envg$serverdown <- FALSE
  tryCatch( { tarch$mlflow$exp_id <- mlflow_set_experiment(tarch$mlflow$exp_name); print(tarch$mlflow$exp_id) }
          , error = function(e) {envg$serverdown <- TRUE})
  print(envg$serverdown)
  if( envg$serverdown ) return( -1 ) 


  if( !tarch$mlflow$padre_creado )
  {
    # creo el experimento padre si hace falta
    tarch$mlflow$padre_exp <- mlflow_start_run( experiment_id= tarch$mlflow$exp_id)

   # siempre las primeras columnas
    mlflow_log_param("mlfowexp", tarch$mlflow$exp_name )
    mlflow_log_param("mlfowrun", tarch$mlflow$run_name )
    mlflow_log_param("usuario", envg$mlog$usuario )
    mlflow_log_param("jerarquia", "padre" )
    mlflow_log_param("maquina", envg$mlog$maquina )
    mlflow_log_metric("fecha", as.numeric(format(t0, "%Y%m%d.%H%M%S")) )

    # las columnas fijas
    mlog_log_mlflow_reg( tarch$cols_fijas )

    tarch$mlflow$padre_creado <- TRUE
    tarch$mlflow$padre_activo <- TRUE
  }

  # restauro el PADRE si hace falta
  if( ! tarch$mlflow$padre_activo )
  {
    mlflow_start_run(run_id= tarch$mlflow$padre_exp$run_uuid )
    tarch$mlflow$padre_activo <- TRUE
  }

  if( parentreplicate )
  {
    mlflow_log_metric("fecha", as.numeric(format(t0, "%Y%m%d.%H%M%S")) )
    mlog_log_mlflow_reg( reg, onlymetrics=TRUE )
  }

  # inicio el hijo   NESTED
  tarch$mlflow$hijo_exp <- mlflow_start_run( nested= TRUE )
  tarch$mlflow$padre_activo <- FALSE

  mlflow_log_param("mlfowexp", tarch$mlflow$exp_name )
  mlflow_log_param("mlfowrun", tarch$mlflow$run_name )
  mlflow_log_param("usuario", envg$mlog$usuario )
  mlflow_log_param("jerarquia", "hijo" )
  mlflow_log_param("maquina", envg$mlog$maquina )
  mlflow_log_metric("fecha", as.numeric(format(t0, "%Y%m%d.%H%M%S")) )

  # las columnas fijas
  mlog_log_mlflow_reg( tarch$cols_fijas )

  # las columnas propias del registro
  mlog_log_mlflow_reg( reg )

  # finalizo el experimento hijo
  mlflow_end_run(run_id= tarch$mlflow$hijo_exp$run_uuid)

  # finalizo el PADRE
  mlflow_end_run(run_id= tarch$mlflow$padre_exp$run_uuid )
  tarch$mlflow$padre_activo <- FALSE

  # persisto en la estructura global
  envg$mlog$larch$archivos[[ archivo ]] <- tarch

  # para poder retomar
  if( envg$mlog$persistir )
    write_yaml( envg$mlog , "mlog.yml" )
}
#------------------------------------------------------------------------------
# funcion que se EXPORTA
# graba a un archivo los componentes de lista
# para el primer registro, escribe antes los titulos
# aqui se agregara  mlflow

mlog_log  <- function( reg, arch=NA, parentreplicate=FALSE, verbose=TRUE )
{
  t0 <- Sys.time()
  archivo <- arch
  if( is.na(arch) ) archivo <- paste0( folder, substitute( reg), ext )


  if( !file.exists( archivo ) )
  {
    # Escribo los titulos
    linea  <- paste0( "fecha\t", 
                      paste( list.names(reg), collapse="\t" ), "\n" )

    cat( linea, file=archivo )
  }

  # escribo el registro
  linea  <- paste0( format(t0, "%Y%m%d.%H%M%S"),  "\t",     # la fecha y hora
                    gsub( ", ", "\t", toString( reg ) ),  "\n" )

  cat( linea, file=archivo, append=TRUE )  # grabo al archivo

  if( verbose )  cat( linea )   # imprimo por pantalla

  # grabo mlflow si corresponde
  if( archivo %in%  names( envg$mlog$larch$archivos ) )
    mlog_log_mlflow( reg, archivo, t0, parentreplicate )
}
#------------------------------------------------------------------------------
# funcion que se EXPORTA

mlog_table_hash <- function( tabla, digitos = 6)
{
  return(  substr(digest(tabla), 1, 6) )
}
#------------------------------------------------------------------------------

# funcion que se EXPORTA

mlog_addfile <- function( archivo, mlflow_exp, mlflow_run, cols_fijas)
{
  # si ya existe, lo elimino
  if( ! archivo %in% names(envg$mlog$larch$archivos) )
  {
    tarch <- list()

    tarch$nom_arch <- archivo
    tarch$cols_fijas <- cols_fijas
    tarch$mlflow$exp_name <- mlflow_exp
    tarch$mlflow$run_name <- mlflow_run
    tarch$mlflow$padre_creado <- FALSE

    envg$mlog$larch$archivos[[ archivo ]] <- tarch
    envg$mlog$larch$qty <- envg$mlog$larch$qty + 1
  }
}
#------------------------------------------------------------------------------
# funcion que se EXPORTA

mlog_init <- function( persistir=TRUE, recreate=FALSE )
{
  if( recreate==FALSE & file.exists( "mlog.yml" ) )
  {
    envg$mlog <- read_yaml( "mlog.yml" )
  }  else {
    envg$mlog <- list()
    envg$mlog$larch$qty <- 0
    envg$mlog$larch$archivos <- list()
  }
  
  envg$mlog$mlflow$iniciado <- FALSE
  envg$mlog$usuario <- Sys.info()["user"]
  envg$mlog$maquina <- Sys.info()["nodename"]
  envg$mlog$persistir <- persistir

}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
