-- Copyright (C) 2015  Guillem Marpons

-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
import           Control.Concurrent.Async.Lifted
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
                                                    mapM, mapM_, null, print)
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
      Primary tipoEleccion ano mes vuelta tipoAmbito ambito
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
      Primary tipoEleccion ano mes codigoCandidatura
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
      Primary tipoEleccion ano mes vuelta codigoCandidatura numeroOrden
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
      numeroMesas                            Int
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
      Primary tipoEleccion ano mes vueltaOPregunta codigoProvincia codigoMunicipio distritoMunicipal
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
      Primary tipoEleccion ano mes vuelta codigoProvincia codigoMunicipio distritoMunicipal codigoCandidatura
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
      numeroMesas                            Int
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
      -- codigoProvincia necessary in PKey because totals per Comunidad have
      -- codigoProvincia = 99.
      Primary tipoEleccion ano mes vueltaOPregunta codigoComunidad codigoProvincia codigoDistritoElectoral
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
      -- codigoProvincia necessary in PKey because totals per Comunidad have
      -- codigoProvincia = 99.
      Primary tipoEleccion ano mes vuelta codigoComunidad codigoProvincia codigoDistritoElectoral codigoCandidatura
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
      -- codigoProvincia necessary in PKey because totals per Comunidad have
      -- codigoProvincia = 99.
      Primary tipoEleccion ano mes vueltaOPregunta codigoComunidad codigoProvincia codigoMunicipio distritoMunicipal codigoSeccion codigoMesa
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
      -- codigoProvincia necessary in PKey because totals per Comunidad have
      -- codigoProvincia = 99.
      Primary tipoEleccion ano mes vuelta codigoComunidad codigoProvincia codigoMunicipio distritoMunicipal codigoSeccion codigoMesa codigoCandidatura
      deriving Show

    DatosMunicipios250          -- 1104aamm.DAT
      tipoMunicipio                          String sqltype=varchar(2)
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      nombreMunicipio                        Text sqltype=varchar(100)
      codigoPartidoJudicial                  Int
      codigoDiputacionProvincial             Int
      codigoComarca                          Int
      poblacionDerecho                       Int
      numeroMesas                            Int
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
      datosOficiales                         String sqltype=varchar(1)
      Primary ano mes vuelta codigoProvincia codigoMunicipio
      deriving Show

    VotosMunicipios250          -- 1204aamm.DAT
      tipoMunicipio                          String sqltype=varchar(2)
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      codigoCandidatura                      Int
      votosCandidatura                       Int
      numeroCandidatos                       Int
      nombreCandidato                        Text sqltype=varchar(75)
      sexo                                   String sqltype=varchar(1)
      fechaNacimiento                        Day Maybe
      dni                                    Text Maybe sqltype=varchar(10)
      votosCandidato                         Int
      elegido                                String sqltype=varchar(1)
      Primary ano mes vuelta codigoProvincia codigoMunicipio codigoCandidatura nombreCandidato
      deriving Show
  |]


-- |
-- = Static data

insertStaticDataIntoDb :: (MonadResource m, MonadIO m, MonadCatch m)
                          =>
                          ReaderT SqlBackend m ()
insertStaticDataIntoDb =
  handleAll (expWhen "inserting rows") $ do
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
  <*> getFecha
  <*> getHora
  <*> getHora
  <*> getHora
  <*> getHora
  <*  getSpaces

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
  <*  getSpaces

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
  <*> ((Just <$> getFecha) <|> (Nothing <$ skip 8))
  <*> getMaybeDni
  <*> getElegido
  <*  getSpaces

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
  <*> (snd <$> getPoblacionDerecho 8)
  <*> (snd <$> getNumeroMesas 5)
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
  <*  getSpaces

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
  <*  getSpaces

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
  <*> (snd <$> getPoblacionDerecho 8)
  <*> (snd <$> getNumeroMesas 5)
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
  <*  getSpaces

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
  <*  getSpaces

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
  <*  getSpaces

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
  <*  getSpaces

getDatosMunicipio250 :: Get DatosMunicipios250
getDatosMunicipio250 =
  DatosMunicipios250
  <$> getTipoMunicipio
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVueltaOPregunta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> getNombreMunicipio
  <*> (snd <$> getCodigoPartidoJudicial)
  <*> (snd <$> getCodigoDiputacionProvincial)
  <*> (snd <$> getCodigoComarca)
  <*> (snd <$> getPoblacionDerecho 3)
  <*> (snd <$> getNumeroMesas 2)
  <*> (snd <$> getCensoINE 3)
  <*> (snd <$> getCensoEscrutinio 3)
  <*> (snd <$> getCensoResidentesExtranjeros 3)
  <*> (snd <$> getVotantesResidentesExtranjeros 3)
  <*> (snd <$> getVotantesPrimerAvanceParticipacion 3)
  <*> (snd <$> getVotantesSegundoAvancePArticipacion 3)
  <*> (snd <$> getVotosEnBlanco 3)
  <*> (snd <$> getVotosNulos 3)
  <*> (snd <$> getVotosACandidaturas 3)
  <*> (snd <$> getNumeroEscanos 2)
  <*> getDatosOficiales
  <*  getSpaces

getVotosMunicipio250 :: Get VotosMunicipios250
getVotosMunicipio250 =
  mk VotosMunicipios250
  <$> getTipoMunicipio
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVuelta)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getVotos 3)      -- votosCandidatura
  <*> (snd <$> getNumeroCandidatos 2)
  <*> getNombreCandidato
  <*> getSexo
  <*> ((Just <$> getFecha) <|> (Nothing <$ skip 8))
  <*> (rightFields <|> wrongFields)
  <*  getSpaces
  where
    mk ctor t a m v p c c' v' n n' s f (d, v'', e) =
      ctor t a m v p c c' v' n n' s f d v'' e
    rightFields =
      (,,)
      <$> getMaybeDni
      <*> (snd <$> getVotos 3)  -- votosCandidato
      <*> getElegido
    -- Some files have a missing byte in dni and an extra space at line end
    wrongFields =
      (,,)
      <$> (Nothing <$ skip 9)
      <*> (snd <$> getVotos 3)  -- votosCandidato
      <*> getElegido
      <*  skip 1

getSpaces :: Get String
getSpaces = many getSpace

getSpace :: Get Char
getSpace = do
  bs <- getByteString 1
  if B8.null (B8.takeWhile (== ' ') bs)
    then empty
    else return ' '

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

getFecha :: Get Day
getFecha = do
  d <- snd <$> getInt 2
  m <- snd <$> getInt 2
  y <- fromIntegral . snd <$> getInt 4
  if d == 0 || m == 0 then empty else return $ fromGregorian y m d

-- | Error if reads "hh:mm" where @hh > 23@ or @mm > 59@.
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

getNombreMunicipioODistrito, getNombreMunicipio :: Get Text
getNombreMunicipioODistrito = getText 100
getNombreMunicipio = getText 100

getCodigoDistritoElectoral :: Get (Text, Int)
getCodigoDistritoElectoral = getInt 1

getCodigoPartidoJudicial ::  Get (Text, Int)
getCodigoPartidoJudicial = getInt 3

getCodigoDiputacionProvincial :: Get (Text, Int)
getCodigoDiputacionProvincial = getInt 3

getCodigoComarca :: Get (Text, Int)
getCodigoComarca = getInt 3

getPoblacionDerecho :: Int -> Get (Text, Int)
getPoblacionDerecho = getInt

getNumeroMesas :: Int -> Get (Text, Int)
getNumeroMesas = getInt

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

getMaybeDni :: Get (Maybe Text)
getMaybeDni = do
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

getTipoMunicipio :: Get String
getTipoMunicipio = B8.unpack <$> getByteString 2

-- | Given a number of bytes to read, gets and 'Int' (into the Get monad) both
-- as a number and as Text. It fails if some of the read bytes is not a decimal
-- digit or fewer than @n@ bytes are left in the input.
getInt :: Int -> Get (Text, Int)
getInt n = do
  bs <- getByteString n
  case B8.readInt bs of
    Just (i, bs') | B8.null bs' -> return (T.pack (B8.unpack bs), i)
    _                           -> empty

-- | Given a number of bytes to read, gets (into the Get monad) the end-stripped
-- 'Text' represented by those bytes. It fails if fewer than @n@ bytes are left
-- in the input.
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
main = execParser options' >>= \(Options b d u p g) -> do
  haveDir <- F.isDirectory b
  if haveDir
    then runNoLoggingT $ withPostgresqlPool (pgConnOpts d u p) 10 $ \pool -> do

           -- Migration and insertion of static data
           liftIO $ flip runSqlPersistMPool pool $ do
             runMigration migrateAll
             insertStaticDataIntoDb

           -- Insertion of dynamic data, one connection per file
           datFiles <- liftIO $ F.listDirectory b >>= filterM isDatFile
           if not (null datFiles)
             then do _ <- mapConcurrently (readFileIntoDb' g pool) datFiles
                     return ()
             else liftIO $ putStrLn $ "Failed: no .DAT files found in " ++ show b

    else putStrLn $ "Failed: " ++ show b ++ " is not a directory"
  where
    options' = info (helper <*> options) helpMessage
    pgConnOpts d u p = B8.pack $ concat [d, u, p]
    isDatFile f = do
      let hasDatExtension = T.toUpper (fromMaybe "" (F.extension f)) == "DAT"
      liftM (hasDatExtension &&) (F.isFile f)
    readFileIntoDb' g pool f = liftIO $ flip runSqlPersistMPool pool $
      case head2 f of
        "02" -> readFileIntoDb f g getProcesoElectoral
        "03" -> readFileIntoDb f g getCandidatura
        -- "04" -> readFileIntoDb f g getCandidato
        "05" -> readFileIntoDb f g getDatosMunicipio
        "06" -> readFileIntoDb f g getVotosMunicipio
        "07" -> readFileIntoDb f g getDatosAmbitoSuperior
        "08" -> readFileIntoDb f g getVotosAmbitoSuperior
        "09" -> readFileIntoDb f g getDatosMesa
        -- "10" -> readFileIntoDb f g getVotosMesa
        "11" -> readFileIntoDb f g getDatosMunicipio250
        "12" -> readFileIntoDb f g getVotosMunicipio250
        _    -> return ()
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
sinkToDb = awaitForever $ \c -> lift $ insert_ c

-- | Also grants access to the updated table for users in the second argument.
readFileIntoDb :: forall a m.
                  ( MonadResource m, MonadIO m, MonadCatch m, MonadLogger m
                  , MonadBaseControl IO m
                  , PersistEntity a, PersistEntityBackend a ~ SqlBackend)
                  =>
                  F.FilePath -> [String] -> Get a -> ReaderT SqlBackend m ()
readFileIntoDb file users fGet =
  handleAll (expWhen "inserting rows") $ do
    liftIO $ putStrLn $ "inserting from " ++ show file
    sourceFile file $= filterWhitespace =$= conduitGet fGet $$ sinkToDb
    -- Works even with empty list!!
    let name = T.filter (/='"') $ tableName (head ([] :: [a]))
    liftIO $ putStrLn $ "Inserted to " ++ show name
    forM_ users $ \user_ ->
      handleAll (expWhen ("granting access privileges for user " ++ user_)) $
      grantAccess name user_
  where
    -- This filtering is only necessary because some files don't follow specs in
    -- 'doc' dir.
    filterWhitespace = filterE (/= 10)

expWhen :: (MonadIO m, MonadCatch m) => String -> SomeException -> m ()
expWhen msg e = do
  let text = "\n\n" ++ "Caught when " ++ msg ++ " :" ++ show e ++ "\n\n"
  liftIO $ putStrLn text
