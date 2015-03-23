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
import           Data.Time.Calendar
import           Data.Time.LocalTime
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
      codigoTipoFichero                      Int
      tipoFichero                            Text
      Primary codigoTipoFichero
      deriving Show

    TiposDeProcesoElectoral
      codigoTipoProcesoElectoral             Int
      tipoProcesoElectoral                   Text
      Primary codigoTipoProcesoElectoral
      deriving Show

    ComunidadesAutonomas
      codigoComunidad                        Int
      comunidad                              Text
      Primary codigoComunidad
      deriving Show

    DistritosElectorales
      codigoTipoProcesoElectoral             Int
      codigoProvincia                        Int
      codigoDistritoElectoral                Int
      provincia                              Text
      distritoElectoral                      Text
      Primary codigoTipoProcesoElectoral codigoProvincia codigoDistritoElectoral
      deriving Show

    -- In the following tables use a multi-valued unique instead of a primary
    -- key because PKEY + repsert fails (probably a bug), and upsert is easier
    -- to use (no need of constructing a Key).

    ProcesosElectorales         -- 02xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      tipoAmbito                             String sqltype=varchar(1)
      ambito                                 Int
      fecha                                  Day
      horaApertura                           TimeOfDay
      horaCierre                             TimeOfDay
      horaPrimerAvanceParticipacion          TimeOfDay
      horaSegundoAvanceParticipacion         TimeOfDay
      UniqueProcesosElectorales tipoEleccion ano mes vuelta tipoAmbito ambito
      deriving Show

    Candidaturas                -- 03xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      codigoCandidatura                      Int
      siglas                                 Text sqltype=varchar(50)
      denominacion                           Text sqltype=varchar(150)
      codigoCandidaturaProvincial            Int
      codigoCandidaturaAutonomico            Int
      codigoCandidaturaNacional              Int
      UniqueCandidaturas tipoEleccion ano mes codigoCandidatura
      deriving Show

    Candidatos                  -- 04xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoProvincia                        Int
      codigoDistritoElectoral                Int
      codigoMunicipio                        Int
      codigoCandidatura                      Int
      numeroOrden                            Int
      tipoCandidato                          String sqltype=varchar(1)
      nombreCandidato                        Text sqltype=varchar(75)
      sexo                                   String sqltype=varchar(1)
      fechaNacimiento                        Day Maybe
      dni                                    Text Maybe sqltype=varchar(10)
      elegido                                String sqltype=varchar(1)
      UniqueCandidatos tipoEleccion ano mes vuelta codigoCandidatura numeroOrden
      deriving Show

    DatosMunicipios             -- 05xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vueltaOPregunta                        Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      distritoMunicipal                      Int
      nombreMunicipioODistrito               Text sqltype=varchar(100)
      codigoDistritoElectoral                Int
      codigoPartidoJudicial                  Int
      codigoDiputacionProvincial             Int
      codigoComarca                          Int
      poblacionDerecho                       Int
      numMesas                               Int
      censoIne                               Int
      censoEscrutinio                        Int
      censoResidentesExtranjeros             Int
      votantesResidentesExtranejros          Int
      votantesPrimerAvanceParticipacion      Int
      votantesSegundoAvanceParticipacion     Int
      votosEnBlanco                          Int
      votosNulos                             Int
      votosACandidaturas                     Int
      numeroEscanos                          Int
      votosAfirmativos                       Int
      votosNegativos                         Int
      datosOficiales                         String sqltype=varchar(1)
      UniqueDatosMunicipios tipoEleccion ano mes vueltaOPregunta codigoProvincia codigoMunicipio distritoMunicipal
      deriving Show

    VotosMunicipios             -- 06xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      distritoMunicipal                      Int
      codigoCandidatura                      Int
      votos                                  Int
      numeroCandidatos                       Int
      UniqueVotosMunicipios tipoEleccion ano mes vuelta codigoProvincia codigoMunicipio distritoMunicipal codigoCandidatura
      deriving Show

    DatosAmbitoSuperior         -- 07xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vueltaOPregunta                        Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoDistritoElectoral                Int
      nombreAmbitoTerritorial                Text sqltype=varchar(50)
      poblacionDerecho                       Int
      numMesas                               Int
      censoIne                               Int
      censoEscrutinio                        Int
      censoResidentesExtranjeros             Int
      votantesResidentesExtranejros          Int
      votantesPrimerAvanceParticipacion      Int
      votantesSegundoAvanceParticipacion     Int
      votosEnBlanco                          Int
      votosNulos                             Int
      votosACandidaturas                     Int
      numeroEscanos                          Int
      votosAfirmativos                       Int
      votosNegativos                         Int
      datosOficiales                         String sqltype=varchar(1)
      UniqueDatosAmbitoSuperior tipoEleccion ano mes vueltaOPregunta codigoProvincia codigoDistritoElectoral
      deriving Show

    VotosAmbitoSuperior         -- 08xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoDistritoElectoral                Int
      codigoCandidatura                      Int
      votos                                  Int
      numeroCandidatos                       Int
      UniqueVotosAmbitoSuperior tipoEleccion ano mes vuelta codigoProvincia codigoDistritoElectoral codigoCandidatura
      deriving Show

    DatosMesas                  -- 09xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vueltaOPregunta                        Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      distritoMunicipal                      Int
      codigoSeccion                          String sqltype=varchar(4)
      codigoMesa                             String sqltype=varchar(1)
      censoIne                               Int
      censoEscrutinio                        Int
      censoResidentesExtranjeros             Int
      votantesResidentesExtranejros          Int
      votantesPrimerAvanceParticipacion      Int
      votantesSegundoAvanceParticipacion     Int
      votosEnBlanco                          Int
      votosNulos                             Int
      votosACandidaturas                     Int
      votosAfirmativos                       Int
      votosNegativos                         Int
      datosOficiales                         String sqltype=varchar(1)
      UniqueDatosMesas tipoEleccion ano mes vueltaOPregunta codigoProvincia codigoMunicipio distritoMunicipal codigoSeccion codigoMesa
      deriving Show

    VotosMesas                  -- 10xxaamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      distritoMunicipal                      Int
      codigoSeccion                          String sqltype=varchar(4)
      codigoMesa                             String sqltype=varchar(1)
      codigoCandidatura                      Int
      votos                                  Int
      UniqueVotosMesas tipoEleccion ano mes vuelta codigoProvincia codigoMunicipio distritoMunicipal codigoSeccion codigoMesa codigoCandidatura
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

getProcesoElectoral :: Get ProcesosElectorales
getProcesoElectoral =
  ProcesosElectorales
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVuelta)
  <*> getTipoAmbito
  <*> (snd <$> getAmbito)
  <*> (fromJust <$> getFecha)
  <*> getHora
  <*> getHora
  <*> getHora
  <*> getHora

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

getCandidato :: Get Candidatos
getCandidato =
  Candidatos
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVuelta)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoDistritoElectoral)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getNumeroOrden)
  <*> getTipoCandidato
  <*> getNombreCandidato
  <*> getSexo
  <*> getFecha
  <*> getDni
  <*> getElegido

getDatosMunicipio :: Get DatosMunicipios
getDatosMunicipio =
  DatosMunicipios
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVueltaOPregunta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getDistritoMunicipal)
  <*> getNombreMunicipioODistrito
  <*> (snd <$> getCodigoDistritoElectoral)
  <*> (snd <$> getCodigoPartidoJudicial)
  <*> (snd <$> getCodigoDiputacionProvincial)
  <*> (snd <$> getCodigoComarca)
  <*> (snd <$> getPoblacionDerecho)
  <*> (snd <$> getNumeroMesas)
  <*> (snd <$> getCensoINE 8)
  <*> (snd <$> getCensoEscrutinio 8)
  <*> (snd <$> getCensoResidentesExtranjeros 8)
  <*> (snd <$> getVotantesResidentesExtranjeros 8)
  <*> (snd <$> getVotantesPrimerAvanceParticipacion 8)
  <*> (snd <$> getVotantesSegundoAvancePArticipacion 8)
  <*> (snd <$> getVotosEnBlanco 8)
  <*> (snd <$> getVotosNulos 8)
  <*> (snd <$> getVotosACandidaturas 8)
  <*> (snd <$> getNumeroEscanos 3)
  <*> (snd <$> getVotosAfirmativos 8)
  <*> (snd <$> getVotosNegativos 8)
  <*> getDatosOficiales

getVotosMunicipio :: Get VotosMunicipios
getVotosMunicipio =
  VotosMunicipios
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVuelta)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getDistritoMunicipal)
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getVotos 8)
  <*> (snd <$> getNumeroCandidatos 3)

getDatosAmbitoSuperior :: Get DatosAmbitoSuperior
getDatosAmbitoSuperior =
  DatosAmbitoSuperior
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVueltaOPregunta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoDistritoElectoral)
  <*> getNombreAmbitoTerritorial
  <*> (snd <$> getPoblacionDerecho)
  <*> (snd <$> getNumeroMesas)
  <*> (snd <$> getCensoINE 8)
  <*> (snd <$> getCensoEscrutinio 8)
  <*> (snd <$> getCensoResidentesExtranjeros 8)
  <*> (snd <$> getVotantesResidentesExtranjeros 8)
  <*> (snd <$> getVotantesPrimerAvanceParticipacion 8)
  <*> (snd <$> getVotantesSegundoAvancePArticipacion 8)
  <*> (snd <$> getVotosEnBlanco 8)
  <*> (snd <$> getVotosNulos 8)
  <*> (snd <$> getVotosACandidaturas 8)
  <*> (snd <$> getNumeroEscanos 6)
  <*> (snd <$> getVotosAfirmativos 8)
  <*> (snd <$> getVotosNegativos 8)
  <*> getDatosOficiales

getVotosAmbitoSuperior :: Get VotosAmbitoSuperior
getVotosAmbitoSuperior =
  VotosAmbitoSuperior
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVuelta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoDistritoElectoral)
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getVotos 8)
  <*> (snd <$> getNumeroCandidatos 5)

getDatosMesa :: Get DatosMesas
getDatosMesa =
  DatosMesas
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVueltaOPregunta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getDistritoMunicipal)
  <*> getCodigoSeccion
  <*> getCodigoMesa
  <*> (snd <$> getCensoINE 7)
  <*> (snd <$> getCensoEscrutinio 7)
  <*> (snd <$> getCensoResidentesExtranjeros 7)
  <*> (snd <$> getVotantesResidentesExtranjeros 7)
  <*> (snd <$> getVotantesPrimerAvanceParticipacion 7)
  <*> (snd <$> getVotantesSegundoAvancePArticipacion 7)
  <*> (snd <$> getVotosEnBlanco 7)
  <*> (snd <$> getVotosNulos 7)
  <*> (snd <$> getVotosACandidaturas 7)
  <*> (snd <$> getVotosAfirmativos 7)
  <*> (snd <$> getVotosNegativos 7)
  <*> getDatosOficiales

getVotosMesa :: Get VotosMesas
getVotosMesa =
  VotosMesas
  <$> (snd <$> getTipoEleccion)
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVuelta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getDistritoMunicipal)
  <*> getCodigoSeccion
  <*> getCodigoMesa
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getVotos 7)

skipToNextLineAfter :: Get a -> Get a
skipToNextLineAfter item = item <* skip 1

getTipoEleccion :: Get (Text, Int)
getTipoEleccion = getInt 2

getAno :: Get (Text, Int)
getAno = getInt 4

getMes :: Get (Text, Int)
getMes = getInt 2

getVuelta :: Get (Text, Int)
getVuelta = getInt 1

getTipoAmbito :: Get String
getTipoAmbito = B8.unpack <$> getByteString 1

getAmbito :: Get (Text, Int)
getAmbito = getInt 2

getFecha :: Get (Maybe Day)
getFecha =
  mkFecha
  <$> (snd <$> getInt 2)
  <*> (snd <$> getInt 2)
  <*> (fromIntegral . snd <$> getInt 4)
  where
    mkFecha 0 0 _ = Nothing
    mkFecha d m y = Just $ fromGregorian y m d

getHora :: Get TimeOfDay
getHora =
  TimeOfDay
  <$> (snd <$> getInt 2)
  <*  skip 1                    -- ":"
  <*> (snd <$> getInt 2)
  <*> pure 0

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

getDistritoMunicipal :: Get (Text, Int)
getDistritoMunicipal = getInt 2

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

getCensoINE :: Int -> Get (Text, Int)
getCensoINE = getInt

getCensoEscrutinio :: Int -> Get (Text, Int)
getCensoEscrutinio = getInt

getCensoResidentesExtranjeros :: Int -> Get (Text, Int)
getCensoResidentesExtranjeros = getInt

getVotantesResidentesExtranjeros :: Int -> Get (Text, Int)
getVotantesResidentesExtranjeros = getInt

getVotantesPrimerAvanceParticipacion :: Int -> Get (Text, Int)
getVotantesPrimerAvanceParticipacion = getInt

getVotantesSegundoAvancePArticipacion :: Int -> Get (Text, Int)
getVotantesSegundoAvancePArticipacion = getInt

getVotosEnBlanco :: Int -> Get (Text, Int)
getVotosEnBlanco = getInt

getVotosNulos :: Int -> Get (Text, Int)
getVotosNulos = getInt

getVotosACandidaturas :: Int -> Get (Text, Int)
getVotosACandidaturas = getInt

getNumeroEscanos :: Int -> Get (Text, Int)
getNumeroEscanos = getInt

getVotosAfirmativos :: Int -> Get (Text, Int)
getVotosAfirmativos = getInt

getVotosNegativos :: Int -> Get (Text, Int)
getVotosNegativos = getInt

getDatosOficiales :: Get String
getDatosOficiales = B8.unpack <$> getByteString 1

getNumeroOrden :: Get (Text, Int)
getNumeroOrden = getInt 3

getTipoCandidato :: Get String
getTipoCandidato = B8.unpack <$> getByteString 1

-- | Try to read person names as one single 'Text', independently of which of
-- the following cases we are faced on:
--
-- 1. Name and two surnames are split up into three pieces of 25 bytes length.
-- 2. All the 75 bytes are a single piece of data with the name and two
-- surnames.
--
-- There is one situation in which the following algorithm does the wrong thing:
-- we are in case (1) and some of the two first pieces of text are using all the
-- 25 bytes. Then, we will concat the pieces without the necessary white-space.
getNombreCandidato :: Get Text
getNombreCandidato =
  (T.unwords . T.words . T.pack . B8.unpack) -- Remove redundant white-space
  <$> ( B8.append
        <$> getByteString 25
        <*> (B8.append <$> getByteString 25 <*> getByteString 25)
      )

getNombre :: Get Text
getNombre = getText 25

getSexo :: Get String
getSexo = B8.unpack <$> getByteString 1

getDni :: Get (Maybe Text)
getDni = do
  t <- getText 10
  if T.all (== ' ') t
    then return Nothing
    else return $ Just t

getElegido :: Get String
getElegido = B8.unpack <$> getByteString 1

getVotos :: Int -> Get (Text, Int)
getVotos = getInt

getNumeroCandidatos :: Int -> Get (Text, Int)
getNumeroCandidatos = getInt

getNombreAmbitoTerritorial :: Get Text
getNombreAmbitoTerritorial = getText 50

getCodigoSeccion :: Get String
getCodigoSeccion = B8.unpack <$> getByteString 4

getCodigoMesa :: Get String
getCodigoMesa = B8.unpack <$> getByteString 1

-- | Given a number of bytes to read, gets and 'Int' (into the Get monad) both
-- as a number and as Text. WARNING: partial function, uses 'read', so it fails
-- if some of the obtained bytes is not a decimal digit, or fewer than @n@ bytes
-- are left in the input.
getInt :: Int -> Get (Text, Int)
getInt n = (T.pack &&& read) . B8.unpack <$> getByteString n

-- | Given a number of bytes to read, gets (into the Get monad) the end-stripped
-- 'Text' represented by those bytes. WARNING: partial function, it fails if
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
      if haveDir
        then do
        runMigration migrateAll
        insertStaticDataIntoDb
        datFiles <- liftIO $ F.listDirectory b >>= filterM isDatFile
        if not (null datFiles)
          then forM_ datFiles $ \f -> case head2 f of
          "02" -> readFileIntoDb f g getProcesoElectoral
          "03" -> readFileIntoDb f g getCandidatura
          "04" -> readFileIntoDb f g getCandidato
          "05" -> readFileIntoDb f g getDatosMunicipio
          "06" -> readFileIntoDb f g getVotosMunicipio
          "07" -> readFileIntoDb f g getDatosAmbitoSuperior
          "08" -> readFileIntoDb f g getVotosAmbitoSuperior
          "09" -> readFileIntoDb f g getDatosMesa
          "10" -> readFileIntoDb f g getVotosMesa
          _    -> return ()
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

-- | Also grants access to the updated table for users in the second argument.
readFileIntoDb :: forall a m.
                  ( MonadResource m, MonadIO m, MonadCatch m, MonadLogger m
                  , MonadBaseControl IO m
                  , PersistEntity a, PersistEntityBackend a ~ SqlBackend)
                  =>
                  F.FilePath -> [String] -> Get a -> ReaderT SqlBackend m ()
readFileIntoDb file users fGet =
  handleAll (expWhen "upserting rows") $ do
    sourceFile file $= conduitGet (skipToNextLineAfter fGet) $$ sinkToDb
    -- Works even with empty list!!
    let name = T.filter (/='"') $ tableName (head ([] :: [a]))
    liftIO $ putStr "Upserted to " >> print name
    forM_ users $ \user_ ->
      handleAll (expWhen ("granting access privileges for user " ++ user_)) $
      grantAccess name user_

expWhen :: (MonadIO m, MonadCatch m) => String -> SomeException -> m ()
expWhen msg e = do
  liftIO $ put2Ln >> putStr "Caught when " >> putStr msg >> putStr " :"
  liftIO $ print e >> put2Ln
  where
    put2Ln = putStrLn "" >> putStrLn ""
