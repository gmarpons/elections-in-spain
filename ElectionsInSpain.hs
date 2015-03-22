{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
module Main where

import           Control.Applicative
import           Control.Arrow
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class            (MonadIO, liftIO)
import           Control.Monad.Logger
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.Resource
import           Data.Binary
import           Data.Binary.Get
import qualified Data.ByteString.Char8             as B8
import           Data.Conduit
import           Data.Conduit.Combinators          hiding (concat, filterM,
                                                    print, null, mapM_)
import           Data.Conduit.Serialization.Binary
import           Data.Maybe
import           Data.String
import           Data.Text                         (Text)
import qualified Data.Text                         as T
import           Database.Persist                  hiding (get)
import           Database.Persist.Postgresql       hiding (get)
import           Database.Persist.TH
import qualified Filesystem                        as F
import qualified Filesystem.Path.CurrentOS         as F
import           Options.Applicative


-- |
-- = DB schema definition

share
  [ mkPersist sqlSettings { mpsPrefixFields   = True
                          , mpsGeneric        = False
                          , mpsGenerateLenses = False
                          }
  , mkMigrate "migrateAll"
  ]
  [persistLowerCase|
    TiposDeFichero
      codigo_tipo_fichero                    Int
      tipo_fichero                           Text
      Primary codigo_tipo_fichero
      deriving Show

    TiposDeProcesoElectoral
      codigo_tipo_proceso_electoral          Int
      tipo_proceso_electoral                 Text
      Primary codigo_tipo_proceso_electoral
      deriving Show

    ComunidadesAutonomas
      codigo_comunidad                       Int
      comunidad                              Text
      Primary codigo_comunidad
      deriving Show

    DistritosElectorales
      codigo_tipo_proceso_electoral          Int
      codigo_provincia                       Int
      codigo_distrito_electoral              Int
      provincia                              Text
      distrito_electoral                     Text
      Primary codigo_tipo_proceso_electoral codigo_provincia codigo_distrito_electoral
      deriving Show

    Candidaturas
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      codigo_candidatura                     Int
      siglas                                 Text sqltype=varchar(50)
      denominacion                           Text sqltype=varchar(150)
      codigo_candidatura_provincial          Int
      codigo_candidatura_autonomico          Int
      codigo_candidatura_nacional            Int
      -- Use a unique multi-value instead of a primary key because PKEY +
      -- repsert fails (probably a bug)
      UniqueCandidaturas tipoEleccion ano mes codigo_candidatura
      deriving Show

    DatosComunesMunicipios
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vueltaOPregunta                        Int
      codigo_comunidad                       Int
      codigo_provincia                       Int
      codigo_municipio                       Int
      num_distrito                           Int
      nombre_municipio_o_distrito            Text sqltype=varchar(100)
      codigo_distrito_electoral              Int
      codigo_partido_judicial                Int
      codigo_diputacion_provincial           Int
      codigo_comarca                         Int
      poblacion_derecho                      Int
      num_mesas                              Int
      censo_ine                              Int
      censo_escrutinio                       Int
      censo_residentes_extranjeros           Int
      votantes_residentes_extranejros        Int
      votantes_primer_avance_participacion   Int
      votantes_segundo_avance_participacion  Int
      votos_en_blanco                        Int
      votos_nulos                            Int
      votos_a_candidaturas                   Int
      numero_escanos                         Int
      votos_afirmativos                      Int
      votos_negativos                        Int
      datos_oficiales                        String sqltype=varchar(1)
      UniqueDatosComunesMunicipios tipoEleccion ano mes vueltaOPregunta codigo_comunidad codigo_provincia codigo_municipio num_distrito
      deriving Show
  |]


-- |
-- = Static data

insertStaticDataIntoDb :: (MonadResource m, MonadIO m, MonadCatch m)
                          =>
                          ReaderT SqlBackend m ()
insertStaticDataIntoDb =
  handleAll (expWhen "upserting rows") $ do
    -- Reinsert all (after delete) on every execution
    deleteWhere ([] :: [Filter TiposDeFichero])
    mapM_ insert_
      [ TiposDeFichero  1 "Control."
      , TiposDeFichero  2 "Identificación del proceso electoral."
      , TiposDeFichero  3 "Candidaturas."
      , TiposDeFichero  4 "Candidatos."
      , TiposDeFichero  5 "Datos globales de ámbito municipal."
      , TiposDeFichero  6 "Datos de candidaturas de ámbito municipal."
      , TiposDeFichero  7 "Datos globales de ámbito superior al municipio."
      , TiposDeFichero  8 "Datos de candidaturas de ámbito superior al municipio."
      , TiposDeFichero  9 "Datos globales de mesas."
      , TiposDeFichero 10 "Datos de candidaturas de mesas."
      , TiposDeFichero 11 "Datos globales de municipios menores de 250 habitantes (en  elecciones municipales)."
      , TiposDeFichero 12 "Datos de candidaturas de municipios menores de 250 habitantes (en elecciones municipales)."
      ]
    deleteWhere ([] :: [Filter TiposDeProcesoElectoral])
    mapM_ insert_
      [ TiposDeProcesoElectoral  1 "Referéndum."
      , TiposDeProcesoElectoral  2 "Elecciones al Congreso de los Diputados."
      , TiposDeProcesoElectoral  3 "Elecciones al Senado."
      , TiposDeProcesoElectoral  4 "Elecciones Municipales."
      , TiposDeProcesoElectoral  5 "Elecciones Autonómicas."
      , TiposDeProcesoElectoral  6 "Elecciones a Cabildos Insulares."
      , TiposDeProcesoElectoral  7 "Elecciones al Parlamento Europeo."
      , TiposDeProcesoElectoral 10 "Elecciones a Partidos Judiciales y Diputaciones Provinciales."
      , TiposDeProcesoElectoral 15 "Elecciones a Juntas Generales."
      ]
    deleteWhere ([] :: [Filter ComunidadesAutonomas])
    mapM_ insert_
      [ ComunidadesAutonomas  1  "Andalucía"
      , ComunidadesAutonomas  2  "Aragón"
      , ComunidadesAutonomas  3  "Asturias"
      , ComunidadesAutonomas  4  "Baleares"
      , ComunidadesAutonomas  5  "Canarias"
      , ComunidadesAutonomas  6  "Cantabria"
      , ComunidadesAutonomas  7  "Castilla - La Mancha"
      , ComunidadesAutonomas  8  "Castilla y León"
      , ComunidadesAutonomas  9  "Cataluña"
      , ComunidadesAutonomas 10  "Extremadura"
      , ComunidadesAutonomas 11  "Galicia"
      , ComunidadesAutonomas 12  "Madrid"
      , ComunidadesAutonomas 13  "Navarra"
      , ComunidadesAutonomas 14  "País Vasco"
      , ComunidadesAutonomas 15  "Región de Murcia"
      , ComunidadesAutonomas 16  "La Rioja"
      , ComunidadesAutonomas 17  "Comunidad Valenciana"
      , ComunidadesAutonomas 18  "Ceuta"
      , ComunidadesAutonomas 19  "Melilla"
      ]
    deleteWhere ([] :: [Filter DistritosElectorales])
    mapM_ insert_
      [ DistritosElectorales  3  7 1 "Baleares" "MALLORCA"
      , DistritosElectorales  3  7 2 "Baleares" "MENORCA"
      , DistritosElectorales  3  7 3 "Baleares" "IBIZA-FORMENTERA"
      , DistritosElectorales  3 35 1 "Las Palmas" "GRAN CANARIA"
      , DistritosElectorales  3 35 2 "Las Palmas" "LANZAROTE"
      , DistritosElectorales  3 35 3 "Las Palmas" "FUERTEVENTURA"
      , DistritosElectorales  3 38 4 "Santa Cruz de Tenerife" "TENERIFE"
      , DistritosElectorales  3 38 5 "Santa Cruz de Tenerife" "LA PALMA"
      , DistritosElectorales  3 38 6 "Santa Cruz de Tenerife" "LA GOMERA"
      , DistritosElectorales  3 38 7 "Santa Cruz de Tenerife" "EL HIERRO"

      , DistritosElectorales  5  7 1 "Baleares" "MALLORCA"
      , DistritosElectorales  5  7 2 "Baleares" "MENORCA"
      , DistritosElectorales  5  7 3 "Baleares" "IBIZA"
      , DistritosElectorales  5  7 4 "Baleares" "FORMENTERA"
      , DistritosElectorales  5 30 1 "Murcia" "PRIMERA"
      , DistritosElectorales  5 30 2 "Murcia" "SEGUNDA"
      , DistritosElectorales  5 30 3 "Murcia" "TERCERA"
      , DistritosElectorales  5 30 4 "Murcia" "CUARTA"
      , DistritosElectorales  5 30 5 "Murcia" "QUINTA"
      , DistritosElectorales  5 33 1 "Asturias" "ORIENTE"
      , DistritosElectorales  5 33 2 "Asturias" "CENTRO"
      , DistritosElectorales  5 33 3 "Asturias" "OCCIDENTE"
      , DistritosElectorales  5 35 1 "Las Palmas" "GRAN CANARIA"
      , DistritosElectorales  5 35 2 "Las Palmas" "LANZAROTE"
      , DistritosElectorales  5 35 3 "Las Palmas" "FUERTEVENTURA"
      , DistritosElectorales  5 38 4 "Santa Cruz de Tenerife" "TENERIFE"
      , DistritosElectorales  5 38 5 "Santa Cruz de Tenerife" "LA PALMA"
      , DistritosElectorales  5 38 6 "Santa Cruz de Tenerife" "LA GOMERA"
      , DistritosElectorales  5 38 7 "Santa Cruz de Tenerife" "EL HIERRO"
      , DistritosElectorales  6 35 1 "Las Palmas" "GRAN CANARIA"
      , DistritosElectorales  6 35 2 "Las Palmas" "LANZAROTE"
      , DistritosElectorales  6 35 3 "Las Palmas" "FUERTEVENTURA"
      , DistritosElectorales  6 38 4 "Santa Cruz de Tenerife" "TENERIFE"
      , DistritosElectorales  6 38 5 "Santa Cruz de Tenerife" "LA PALMA"
      , DistritosElectorales  6 38 6 "Santa Cruz de Tenerife" "LA GOMERA"
      , DistritosElectorales  6 38 7 "Santa Cruz de Tenerife" "EL HIERRO"
      , DistritosElectorales 15  1 1 "Álava" "VITORIA-GASTEIZ"
      , DistritosElectorales 15  1 2 "Álava" "AIRA-AYALA"
      , DistritosElectorales 15  1 3 "Álava" "RESTO"
      , DistritosElectorales 15 20 1 "Guipúzcoa" "DEBA-UROLA"
      , DistritosElectorales 15 20 2 "Guipúzcoa" "BIDASOA-OYARZUN"
      , DistritosElectorales 15 20 3 "Guipúzcoa" "DONOSTIALDEA"
      , DistritosElectorales 15 20 4 "Guipúzcoa" "ORIA"
      , DistritosElectorales 15 48 1 "Vizcaya" "BILBAO"
      , DistritosElectorales 15 48 2 "Vizcaya" "ENCARTACIONES"
      , DistritosElectorales 15 48 3 "Vizcaya" "DURANGO-ARRATIA"
      , DistritosElectorales 15 48 4 "Vizcaya" "BUSTURIA-URIBE"
      ]
    liftIO $ putStrLn "Inserted static data"


-- |
-- = Dynamic data

getCandidatura :: Get Candidaturas
getCandidatura =
  Candidaturas
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getCodigoCandidatura)
  <*> getSiglas
  <*> getDenominacion
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getCodigoCandidatura)

getDatosComunesMunicipio :: Get DatosComunesMunicipios
getDatosComunesMunicipio =
  DatosComunesMunicipios
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVueltaOPregunta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getNumDistrito)
  <*> getNombreMunicipioODistrito
  <*> (snd <$> getCodigoDistritoElectoral)
  <*> (snd <$> getCodigoPartidoJudicial)
  <*> (snd <$> getCodigoDiputacionProvincial)
  <*> (snd <$> getCodigoComarca)
  <*> (snd <$> getPoblacionDerecho)
  <*> (snd <$> getNumeroMesas)
  <*> (snd <$> getCensoINE)
  <*> (snd <$> getCensoEscrutinio)
  <*> (snd <$> getCensoResidentesExtranjeros)
  <*> (snd <$> getVotantesResidentesExtranjeros)
  <*> (snd <$> getVotantesPrimerAvanceParticipacion)
  <*> (snd <$> getVotantesSegundoAvancePArticipacion)
  <*> (snd <$> getVotosEnBlanco)
  <*> (snd <$> getVotosNulos)
  <*> (snd <$> getVotosACandidaturas)
  <*> (snd <$> getNumeroEscanos)
  <*> (snd <$> getVotosAfirmativos)
  <*> (snd <$> getVotosNegativos)
  <*> getDatosOficiales

skipToNextLineAfter :: Get a -> Get a
skipToNextLineAfter item = item <* skip 1

getTipoEleccion :: Get (Text, Int)
getTipoEleccion = getInt 2

getAno :: Get (Text, Int)
getAno = getInt 4

getMes :: Get (Text, Int)
getMes = getInt 2

getCodigoCandidatura :: Get (Text, Int)
getCodigoCandidatura = getInt 6

getSiglas :: Get Text
getSiglas = getText 50

getDenominacion :: Get Text
getDenominacion = getText 150

getVueltaOPregunta :: Get (Text, Int)
getVueltaOPregunta = getInt 1

getCodigoComunidad :: Get (Text, Int)
getCodigoComunidad = getInt 2

getCodigoProvincia :: Get (Text, Int)
getCodigoProvincia = getInt 2

getCodigoMunicipio :: Get (Text, Int)
getCodigoMunicipio = getInt 3

getNumDistrito :: Get (Text, Int)
getNumDistrito = getInt 2

getNombreMunicipioODistrito :: Get Text
getNombreMunicipioODistrito = getText 100

getCodigoDistritoElectoral :: Get (Text, Int)
getCodigoDistritoElectoral = getInt 1

getCodigoPartidoJudicial ::  Get (Text, Int)
getCodigoPartidoJudicial = getInt 3

getCodigoDiputacionProvincial :: Get (Text, Int)
getCodigoDiputacionProvincial = getInt 3

getCodigoComarca :: Get (Text, Int)
getCodigoComarca = getInt 3

getPoblacionDerecho :: Get (Text, Int)
getPoblacionDerecho = getInt 8

getNumeroMesas :: Get (Text, Int)
getNumeroMesas = getInt 5

getCensoINE :: Get (Text, Int)
getCensoINE = getInt 8

getCensoEscrutinio :: Get (Text, Int)
getCensoEscrutinio = getInt 8

getCensoResidentesExtranjeros :: Get (Text, Int)
getCensoResidentesExtranjeros = getInt 8

getVotantesResidentesExtranjeros :: Get (Text, Int)
getVotantesResidentesExtranjeros = getInt 8

getVotantesPrimerAvanceParticipacion :: Get (Text, Int)
getVotantesPrimerAvanceParticipacion = getInt 8

getVotantesSegundoAvancePArticipacion :: Get (Text, Int)
getVotantesSegundoAvancePArticipacion = getInt 8

getVotosEnBlanco :: Get (Text, Int)
getVotosEnBlanco = getInt 8

getVotosNulos :: Get (Text, Int)
getVotosNulos = getInt 8

getVotosACandidaturas :: Get (Text, Int)
getVotosACandidaturas = getInt 8

getNumeroEscanos :: Get (Text, Int)
getNumeroEscanos = getInt 3

getVotosAfirmativos :: Get (Text, Int)
getVotosAfirmativos = getInt 8

getVotosNegativos :: Get (Text, Int)
getVotosNegativos = getInt 8

-- | WARNING: partial function, it fails if fewer than 1 byte is left in the
-- input.
getDatosOficiales :: Get String
getDatosOficiales = B8.unpack <$> getByteString 1

-- | Given a number of bytes to read, gets and @Int@ (into the Get monad) both
-- as a number and as Text. WARNING: partial function, uses @read@, so it fails
-- if some of the obtained bytes is not a decimal digit, or fewer than @n@ bytes
-- are left in the input.
getInt :: Int -> Get (Text, Int)
getInt n = (T.pack &&& read) . B8.unpack <$> getByteString n

-- | Given a number of bytes to read, gets (into the Get monad) the end-stripped
-- @Text@ represented by those bytes. WARNING: partial function, it fails if
-- fewer than @n@ bytes are left in the input.
getText :: Int -> Get Text
getText n = T.stripEnd . T.pack . B8.unpack <$> getByteString n


-- |
-- = Command line options

data Options
  = Options
    { basedir            :: F.FilePath
    , dbname             :: String
    , user               :: String
    , password           :: String
    , usersToGrantAccess :: [String]
    }

options :: Parser Options
options = Options
  <$> option (fromString <$> str)
  ( long "basedir"
    <> short 'b'
    <> metavar "DIRECTORY"
    <> help "Directory containing *.DAT files to process"
  )
  <*> option (str >>= param "dbname")
  ( long "dbname"
    <> short 'd'
    <> metavar "DB"
    <> help "Passes parameter dbname=DB to database connection"
    <> value ""
  )
  <*> option (str >>= param "user")
  ( long "username"
    <> short 'u'
    <> metavar "USER"
    <> help "Passes parameter user=USER to database connection"
    <> value ""
  )
  <*> option (str >>= param "password")
  ( long "password"
    <> short 'p'
    <> metavar "PASSWD"
    <> help "Passes param. password=PASSWD to database connection"
    <> value ""
  )
  <*> option ((fmap T.unpack . T.splitOn "," . T.pack) <$> str)
  ( long "grant-access-to"
    <> short 'g'
    <> metavar "USERS"
    <> help "Comma-separated list of DB users to grant access privileges"
    <> value []
  )
  where
    param _ "" = return ""
    param p s  = return $ p ++ "=" ++ s ++ " "

helpMessage :: InfoMod a
helpMessage =
  fullDesc
  <> progDesc "Connect to database and do things"
  <> header "elections-in-spain - Relational-ize .DAT files\
            \ from www.infoelectoral.interior.es"


-- | = Entry point and auxiliary functions.

-- | Entry point. All SQL commands are run in a single transaction.
main :: IO ()
main = execParser options' >>= \(Options b d u p g) ->
  runNoLoggingT $ withPostgresqlPool (pgConnOpts d u p) 10 $ \pool ->
    liftIO $ flip runSqlPersistMPool pool $ do
      haveDir <- liftIO $ F.isDirectory b
      if haveDir then do
        runMigration migrateAll
        insertStaticDataIntoDb
        datFiles <- liftIO $ F.listDirectory b >>= filterM isDatFile
        if not (null datFiles) then do
          forM_ datFiles $ \f -> case head2 f of
            "03" -> readFileIntoDb f getCandidatura
            "05" -> readFileIntoDb f getDatosComunesMunicipio
            _    -> return ()
          let tables = ["candidaturas"]
          forM_ [(u_, t) | u_ <- g, t <- tables] $ \(user_, table) ->
            handleAll (expWhen ("granting access privileges for user " ++ user_)) $
            grantAccess table user_
          else liftIO $ putStrLn $ "Failed: no .DAT files found in " ++ show b
        else liftIO $ putStrLn $ "Failed: " ++ show b ++ " is not a directory"
  where
    options' = info (helper <*> options) helpMessage
    pgConnOpts d u p = B8.pack $ concat [d, u, p]
    isDatFile f = do
      let hasDatExtension = T.toUpper (fromMaybe "" (F.extension f)) == "DAT"
      liftM (hasDatExtension &&) (F.isFile f)
    head2 file = T.take 2 (either id id (F.toText (F.filename file)))

grantAccess :: (MonadBaseControl IO m, MonadLogger m, MonadIO m)
               =>
               Text -> String -> ReaderT SqlBackend m ()
grantAccess t u = rawExecute (T.concat ["GRANT SELECT ON ", t," TO ", u']) []
  where
    u' = T.pack u

sinkToDb :: ( MonadResource m
            , PersistEntity a, PersistEntityBackend a ~ SqlBackend)
            =>
            Sink a (ReaderT SqlBackend m) ()
sinkToDb = awaitForever $ \c -> lift $ upsert c []

readFileIntoDb :: forall a m.
                  ( MonadResource m, MonadIO m, MonadCatch m
                  , PersistEntity a, PersistEntityBackend a ~ SqlBackend)
                  =>
                  F.FilePath -> Get a -> ReaderT SqlBackend m ()
readFileIntoDb file fGet =
  handleAll (expWhen "upserting rows") $ do
    sourceFile file $= conduitGet (skipToNextLineAfter fGet) $$ sinkToDb
    -- Works even with empty list!!
    let name = T.filter (/='"') $ tableName (head ([] :: [a]))
    liftIO $ putStr "Upserted to " >> print name

expWhen :: (MonadIO m, MonadCatch m) => String -> SomeException -> m ()
expWhen msg e = do
  liftIO $ put2Ln >> putStr "Caught when " >> putStr msg >> putStr " :"
  liftIO $ print e >> put2Ln
  where
    put2Ln = putStrLn "" >> putStrLn ""
