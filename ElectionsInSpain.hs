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

import           Codec.Archive.Zip
import           Control.Applicative
import           Control.Concurrent.Async.Lifted
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class            (MonadIO, liftIO)
import           Control.Monad.Logger
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.Resource
import           Data.Binary
import           Data.Binary.Get
import qualified Data.ByteString.Char8             as BS8
import           Data.Conduit
import qualified Data.Conduit.Combinators          as CC
import qualified Data.Conduit.List                 as CL
import qualified Data.Conduit.Serialization.Binary as CS
import           Data.Maybe
import           Data.String
import           Data.Text                         (Text)
import qualified Data.Text                         as T
import           Data.Time.Calendar
import           Data.Time.LocalTime
import           Database.Persist                  hiding (get)
import           Database.Persist.Postgresql       hiding (get, getTableName)
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

    -- No extra indexes for DistritosElectorales: this table is not expected to
    -- be large.

    -- No indexes for (ano, mes), as we can search by (tipoEleccion, ano, mes)
    -- with PKey's indexes.

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
      nombrePila                             Text Maybe sqltype=varchar(25)
      primerApellido                         Text Maybe sqltype=varchar(25)
      segundoApellido                        Text Maybe sqltype=varchar(25)
      sexo                                   String sqltype=varchar(1)
      fechaNacimiento                        Day Maybe
      dni                                    Text Maybe sqltype=varchar(10)
      elegido                                String sqltype=varchar(1)
      Primary tipoEleccion ano mes vuelta codigoProvincia codigoDistritoElectoral codigoMunicipio codigoCandidatura numeroOrden
      deriving Show

    DatosMunicipios             -- 05xxaamm.DAT and 1104aamm.DAT
      tipoEleccion                           Int       -- 4 if < 250
      ano                                    Int
      mes                                    Int
      -- Called 'vuelta' in < 250
      vueltaOPregunta                        Int
      codigoComunidad                        Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      distritoMunicipal                      Int       -- 99 if < 250
      -- Called 'nombreMunicipio' in < 250
      nombreMunicipioODistrito               Text sqltype=varchar(100)
      codigoDistritoElectoral                Int Maybe -- Nothing if < 250
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
      votosAfirmativos                       Int Maybe -- Nothing if < 250
      votosNegativos                         Int Maybe -- Nothing if < 250
      datosOficiales                         String sqltype=varchar(1)
      -- The following field must be Nothing if > 250
      tipoMunicipio                          String Maybe sqltype=varchar(2)
      Primary tipoEleccion ano mes vueltaOPregunta codigoProvincia codigoMunicipio distritoMunicipal
      deriving Show

    VotosMunicipios             -- 06xxaamm.DAT and 1204aamm.DAT
      tipoEleccion                           Int
      ano                                    Int
      mes                                    Int
      vuelta                                 Int
      codigoProvincia                        Int
      codigoMunicipio                        Int
      distritoMunicipal                      Int       -- 99 if < 250
      codigoCandidatura                      Int
      -- Called 'votos' in > 250
      votosCandidatura                       Int
      numeroCandidatos                       Int
      -- The following field must be "" if > 250
      nombreCandidato                        Text sqltype=varchar(75)
      nombrePila                             Text Maybe sqltype=varchar(25)
      primerApellido                         Text Maybe sqltype=varchar(25)
      segundoApellido                        Text Maybe sqltype=varchar(25)
      -- The 5 that follow: Nothing if > 250 (fechaNacimiento and dni can also
      -- be Noting in some < 250)
      tipoMunicipio                          String Maybe sqltype=varchar(2)
      sexo                                   String Maybe sqltype=varchar(1)
      fechaNacimiento                        Day    Maybe
      dni                                    Text   Maybe sqltype=varchar(10)
      votosCandidato                         Int    Maybe
      elegido                                String Maybe sqltype=varchar(1)
      Primary tipoEleccion ano mes vuelta codigoProvincia codigoMunicipio distritoMunicipal codigoCandidatura nombreCandidato
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

getCandidato :: PersonNameMode -> Get Candidatos
getCandidato mode =
  (\ctor (nc, np, a1, a2) (s, f, mD, e) -> ctor nc np a1 a2 s f mD e)
  <$> getCandidato'
  <*> getNombreCandidato mode
  <*> getCandidato''
  <*  getSpaces
  where
    getCandidato' =
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
    getCandidato'' =
      (,,,)
      <$> getSexo
      <*> ((Just <$> getFecha) <|> (Nothing <$ skip 8))
      <*> getDniMaybe
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
  <*> (Just . snd <$> getCodigoDistritoElectoral)
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
  <*> (Just . snd <$> getVotosAfirmativos 8)
  <*> (Just . snd <$> getVotosNegativos 8)
  <*> getDatosOficiales
  <*> pure Nothing              -- tipoMunicipio
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
  <*> pure ""
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
  <*> pure Nothing
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

getDatosMunicipio250 :: Get DatosMunicipios
getDatosMunicipio250 =
  ((\tm ctor -> ctor (Just tm)) <$> getTipoMunicipio <*>)
  $
  DatosMunicipios
  <$> pure 4                    -- tipoEleccion
  <*> (snd <$> getAno)
  <*> (snd <$> getMes)
  <*> (snd <$> getVueltaOPregunta)
  <*> (snd <$> getCodigoComunidad)
  <*> (snd <$> getCodigoProvincia)
  <*> (snd <$> getCodigoMunicipio)
  <*> pure 99                   -- distritoMunicipal
  <*> getNombreMunicipio
  <*> pure Nothing              -- codigoDistritoElectoral
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
  <*> pure Nothing              -- votosAfirmativos
  <*> pure Nothing              -- votosNegativos
  <*> getDatosOficiales
  <*  getSpaces

getVotosMunicipio250 :: PersonNameMode -> Get VotosMunicipios
getVotosMunicipio250 mode =
  (\tm ctor (nc, np, a1, a2) (s, mF) (mD, vc, e) ->
    ctor nc np a1 a2 (Just tm) (Just s) mF mD (Just vc) (Just e))
  <$> getTipoMunicipio
  <*> getVotosMunicipios'
  <*> getNombreCandidato mode
  <*> getVotosMunicipios''
  <*> getVotosMunicipios'''
  where
    getVotosMunicipios' =
      VotosMunicipios
      <$> pure 4                -- tipoEleccion
      <*> (snd <$> getAno)
      <*> (snd <$> getMes)
      <*> (snd <$> getVuelta)
      <*> (snd <$> getCodigoProvincia)
      <*> (snd <$> getCodigoMunicipio)
      <*> pure 99               -- distritoMunicipal
      <*> (snd <$> getCodigoCandidatura)
      <*> (snd <$> getVotos 3)  -- votosCandidatura
      <*> (snd <$> getNumeroCandidatos 2)
    getVotosMunicipios'' =
      (,)
      <$> getSexo
      <*> ((Just <$> getFecha) <|> (Nothing <$ skip 8))
    getVotosMunicipios''' =
      (rightFields <|> wrongFields)
      <*  getSpaces
    rightFields =
      (,,)
      <$> getDniMaybe
      <*> (snd <$> getVotos 3)  -- votosCandidato
      <*> getElegido
    -- Some files have a missing byte in dni and an extra space at line end
    wrongFields =
      (,,)
      <$> (Nothing <$ skip 9)
      <*> (snd <$> getVotos 3)  -- votosCandidato
      <*> getElegido
      <*  skip 1

data AnyPersistEntityGetFunc
  = forall a. (PersistEntity a, PersistEntityBackend a ~ SqlBackend)
    => MkAPEGF (Get a)
  | forall a. (PersistEntity a, PersistEntityBackend a ~ SqlBackend)
    => MkAPEGF' (PersonNameMode -> Get a)

getAnyPersistEntity :: FileRef -> Maybe AnyPersistEntityGetFunc
getAnyPersistEntity fRef =
  case twoFirstDigits fRef of
    "02" -> Just $ MkAPEGF  getProcesoElectoral
    "03" -> Just $ MkAPEGF  getCandidatura
    "04" -> Just $ MkAPEGF' getCandidato
    "05" -> Just $ MkAPEGF  getDatosMunicipio
    "06" -> Just $ MkAPEGF  getVotosMunicipio
    "07" -> Just $ MkAPEGF  getDatosAmbitoSuperior
    "08" -> Just $ MkAPEGF  getVotosAmbitoSuperior
    "09" -> Just $ MkAPEGF  getDatosMesa
    "10" -> Just $ MkAPEGF  getVotosMesa
    "11" -> Just $ MkAPEGF  getDatosMunicipio250
    "12" -> Just $ MkAPEGF' getVotosMunicipio250
    _    -> Nothing

getSpaces :: Get String
getSpaces = many getSpace

getSpace :: Get Char
getSpace = do
  bs <- getByteString 1
  if BS8.null (BS8.takeWhile (== ' ') bs)
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
getTipoAmbito = BS8.unpack <$> getByteString 1

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
getDatosOficiales = BS8.unpack <$> getByteString 1

getNumeroOrden :: Get (Text, Int)
getNumeroOrden = getInt 3

getTipoCandidato :: Get String
getTipoCandidato = BS8.unpack <$> getByteString 1

class
  (PersistEntity a, PersistEntityBackend a ~ SqlBackend)
  =>
  HasPersonName a
  where
    nameOneField    :: (HasPersonName a) => a -> Text
    nameThreeFields :: (HasPersonName a) => a -> Text
    fields          :: [a -> Maybe Text]
    nameThreeFields e = T.intercalate " " $ fromMaybe "" <$> ap fields [e]

instance HasPersonName Candidatos where
  nameOneField = candidatosNombreCandidato
  fields = [ candidatosNombrePila
           , candidatosPrimerApellido
           , candidatosSegundoApellido ]

instance HasPersonName VotosMunicipios where
  nameOneField = votosMunicipiosNombreCandidato
  fields = [ votosMunicipiosNombrePila
           , votosMunicipiosPrimerApellido
           , votosMunicipiosSegundoApellido ]

-- | Read person names in four different ways, depending on
-- parameter. 'OneFieldName' and 'ThreeFieldsName' are the main alternatives
-- that must be chosen depending on file format. If 'NoInteractionName' is
-- passed, and algorithm is used that tries to read person names as one single
-- 'Text', independently of which of the following cases we are faced on:
--
-- 1. Name and two surnames are split up into three pieces of 25 bytes length.
-- 2. All the 75 bytes are a single piece of data with the name and two
-- surnames.
--
-- There is one situation in which the algorithm does the wrong thing: we are in
-- case (1) and some of the two first pieces of text are using all the 25
-- bytes. Then, we will concat the pieces without the necessary white-space.
--
-- The option 'TryOneAndThreeFieldsName' is used to get both the results of the
-- first two options, to possibly show the results to the user and let her chose
-- between them.
getNombreCandidato
  :: PersonNameMode -> Get (Text, Maybe Text, Maybe Text, Maybe Text)
getNombreCandidato OneFieldName =
  (\t -> (t, Nothing, Nothing, Nothing))
  <$> (T.stripEnd . T.pack . BS8.unpack)
  <$> getByteString 75
getNombreCandidato ThreeFieldsName =
  (\np a1 a2 -> (np <> " " <> a1 <> " " <> a2, Just np, Just a1, Just a2))
  <$> ((T.stripEnd . T.pack . BS8.unpack) <$> getByteString 25)
  <*> ((T.stripEnd . T.pack . BS8.unpack) <$> getByteString 25)
  <*> ((T.stripEnd . T.pack . BS8.unpack) <$> getByteString 25)
getNombreCandidato NoInteractionName =
  (\nc -> (nc, Nothing, Nothing, Nothing))
  <$> (T.unwords . T.words . T.pack . BS8.unpack) -- Remove redundant whitespace
  <$> getByteString 75
getNombreCandidato TryOneAndThreeFieldsName =
  (\np a1 a2 -> ( T.stripEnd (np <> a1 <> a2)
                , Just (T.stripEnd np)
                , Just (T.stripEnd a1)
                , Just (T.stripEnd a2) )
  )
  <$> ((T.pack . BS8.unpack) <$> getByteString 25)
  <*> ((T.pack . BS8.unpack) <$> getByteString 25)
  <*> ((T.pack . BS8.unpack) <$> getByteString 25)

getNombre :: Get Text
getNombre = getText 25

getSexo :: Get String
getSexo = BS8.unpack <$> getByteString 1

getDniMaybe :: Get (Maybe Text)
getDniMaybe = do
  t <- getText 10
  if T.all (== ' ') t
    then return Nothing
    else return $ Just t

getElegido :: Get String
getElegido = BS8.unpack <$> getByteString 1

getVotos :: Int -> Get (Text, Int)
getVotos = getInt

getNumeroCandidatos :: Int -> Get (Text, Int)
getNumeroCandidatos = getInt

getNombreAmbitoTerritorial :: Get Text
getNombreAmbitoTerritorial = getText 50

getCodigoSeccion :: Get String
getCodigoSeccion = BS8.unpack <$> getByteString 4

getCodigoMesa :: Get String
getCodigoMesa = BS8.unpack <$> getByteString 1

getTipoMunicipio :: Get String
getTipoMunicipio = BS8.unpack <$> getByteString 2

-- | Given a number of bytes to read, gets and 'Int' (into the Get monad) both
-- as a number and as Text. It fails if fewer than @n@ bytes are left in the
-- input or some of the read bytes is not a decimal digit, with the exception of
-- all bytes being spaces. In this case, and due to some .DAT files not honoring
-- their specification, (<n spaces>, 0) is returned.
getInt :: Int -> Get (Text, Int)
getInt n = do
  bs <- getByteString n
  case BS8.readInt bs of
    Just (i, bs') | BS8.null bs' -> return (T.pack (BS8.unpack bs), i)
    Nothing                      -> if BS8.all (== ' ') bs
                                    then return (T.pack (BS8.unpack bs), 0)
                                    else empty
    _                            -> empty

-- | Given a number of bytes to read, gets (into the Get monad) the end-stripped
-- 'Text' represented by those bytes. It fails if fewer than @n@ bytes are left
-- in the input.
getText :: Int -> Get Text
getText n = T.stripEnd . T.pack . BS8.unpack <$> getByteString n


-- |
-- = Command line options

data MigrateFlag = Migrate | Don'tMigrate

data InteractiveFlag = Interactive | NoInteractive

data Options
  = Options
    { dbname               :: String
    , user                 :: String
    , password             :: String
    , usersToGrantAccessTo :: [String]
    , migrateFlag          :: MigrateFlag
    , interactiveFlag      :: InteractiveFlag
    , files                :: [F.FilePath]
    }

options :: Parser Options
options = Options
  <$> option (str >>= param "dbname")
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
    <> help "Comma-separated list of DB users to grant access privileges to"
    <> value []
  )
  <*> flagMigrate
  <*> flagInteractive
  <*> some (argument (fromString <$> str) (metavar "FILES..."))
  where
    param _ "" = return ""
    param p s  = return $ p ++ "=" ++ s ++ " "
    flagMigrate =
      caseMigrate
      <$> flag (Just Migrate) (Just Don'tMigrate)
      ( long "no-migration"
        <> help "Disable DB migration and static data insertion"
      )
      <*> flag Nothing (Just Migrate)
      ( long "migration"
        <> help "Enable DB migration and static data insertion (default)"
      )
    caseMigrate (Just Migrate) _              = Migrate
    caseMigrate _              (Just Migrate) = Migrate
    caseMigrate _              _              = Don'tMigrate
    flagInteractive =
      caseInteractive
      <$> flag (Just Interactive) (Just NoInteractive)
      ( long "no-interactive"
        <> help "Disable interaction to choose person name format"
      )
      <*> flag Nothing (Just Interactive)
      ( long "interactive"
        <> help "Enable interaction to choose person name format (default)"
      )
    caseInteractive (Just Interactive) _                  = Interactive
    caseInteractive _                  (Just Interactive) = Interactive
    caseInteractive _                  _                  = NoInteractive

helpMessage :: InfoMod a
helpMessage =
  fullDesc
  <> progDesc "Connect to database and do things"
  <> header "Relational-ize .DAT files\
            \ from www.infoelectoral.interior.es"


-- |
-- = Entry point and auxiliary functions.

-- | Entry point. All SQL commands related to a file are run in a single
-- connection/transaction.
main :: IO ()
main = execParser options' >>= \(Options d u p g migrFlag interFlag filePaths) ->
  runNoLoggingT $ withPostgresqlPool (pgConnOpts d u p) 100 $ \pool -> do

    -- Migration and insertion of static data
    case migrFlag of
      Migrate -> liftIO $ flip runSqlPersistMPool pool $ do
        liftIO $ putStrLn "Migrating database"
        runMigration migrateAll
        liftIO $ putStrLn "Inserting static data"
        insertStaticDataIntoDb
      Don'tMigrate -> do
        liftIO $ putStrLn "Skipping database migration"
        liftIO $ putStrLn "Skipping static data insertion"

    -- Ask for person name syntax
    fileRefsL <- mapM (toFileRefsWithPersonNameMode interFlag) filePaths
    let fileRefs = concat fileRefsL

    -- Insertion of dynamic data
    liftIO $ putStrLn "Inserting dynamic data"
    _ <- mapConcurrently (readFileRefIntoDb g pool) fileRefs
    return ()

  where
    options' = info (helper <*> options) helpMessage
    pgConnOpts d u p = BS8.pack $ concat [d, u, p]

data PersonNameMode
  = OneFieldName
  | ThreeFieldsName
  | NoInteractionName
  | TryOneAndThreeFieldsName
  deriving Show

data FileRef
  = Path F.FilePath
  | ZipEntry F.FilePath Entry

instance Show FileRef where
  show (Path p)       = filePathToStr p
  show (ZipEntry p e) = filePathToStr p <> "[" <> eRelativePath e <> "]"

filePathToStr :: F.FilePath -> String
filePathToStr p = T.unpack $ either id id $ F.toText p

twoFirstDigits :: FileRef -> String
twoFirstDigits (Path p)
  = take 2 $ T.unpack $ either id id $ F.toText $ F.basename p
twoFirstDigits (ZipEntry _p e)
  = twoFirstDigits $ Path $ F.fromText $ T.pack $ eRelativePath e

sourceFileRef
  :: forall m.
     MonadResource m
     => FileRef -> Producer m BS8.ByteString
sourceFileRef (Path p)       = CC.sourceFile p
sourceFileRef (ZipEntry _ e) = CC.sourceLazy (fromEntry e)

-- | Returns a list of @FileRef@s with an associated @PersonNameMode@ that
-- depends on the result of user interaction. In case of .zip files one pair per
-- entry is returned. A @FileRef@ will have 'Nothing' assigned if it doesn't
-- contain person names. In the case of a parsing or I/O error while reading the
-- file (or one of its entries) an empty list is returned.
toFileRefsWithPersonNameMode
  :: forall m.
     (MonadIO m, MonadCatch m, MonadBaseControl IO m)
     =>
     InteractiveFlag -> F.FilePath -> m [(FileRef, Maybe PersonNameMode)]
toFileRefsWithPersonNameMode interFlag filePath = do
  isRegularFile <- liftIO $ F.isFile filePath
  if isRegularFile then runResourceT $
    handleAll (expWhen' $ "reading file " <> show filePath) $
      childRefs (Path filePath) extension (twoFirstDigits (Path filePath))
    else do liftIO $ putStrLn $ "Error: File " ++ show filePath
              ++ " doesn't exist or is not readable"
            return []
  where
    childRefs fRef "DAT" "04" = handle (expParse fRef) $
                                case interFlag of
                                  Interactive -> do
                                    m <-    sourceFileRef fRef
                                         $$ askForPersonNameMode getCandidato'
                                    return [(fRef, Just m)]
                                  NoInteractive ->
                                    return [(fRef, Just NoInteractionName)]
    childRefs fRef "DAT" "12" = handle (expParse fRef) $
                                case interFlag of
                                  Interactive -> do
                                    m <-    sourceFileRef fRef
                                         $$ askForPersonNameMode getVotos'
                                    return [(fRef, Just m)]
                                  NoInteractive ->
                                    return [(fRef, Just NoInteractionName)]
    childRefs fRef "DAT" _    = return [(fRef, Nothing)]
    childRefs _    "ZIP" _    =     sourceDatEntriesFromZipFile filePath
                                $=  CC.mapM recCall
                                =$= CC.concat
                                $$  CL.consume
    childRefs _    _     _    = do liftIO $ putStrLn $ "Error: File "
                                     ++ show filePath
                                     ++ " is not a .DAT or .ZIP file"
                                   return []
    recCall e = let fRef = ZipEntry filePath e
                in childRefs fRef "DAT" (twoFirstDigits fRef)
    extension = T.toUpper (fromMaybe "" (F.extension filePath))
    getCandidato' = getCandidato TryOneAndThreeFieldsName
    getVotos'     = getVotosMunicipio250 TryOneAndThreeFieldsName
    expParse fRef exception = do
      expWhenParseError ("reading file " <> show fRef) exception
      return []
    expWhen' msg exception = do { expWhen msg exception; return [] }

sourceDatEntriesFromZipFile
  :: forall m.
     (MonadResource m, MonadCatch m)
     =>
     F.FilePath -> Producer m Entry
sourceDatEntriesFromZipFile filePath =
      CC.sourceFile filePath
  $=  CS.conduitDecode
  =$= CC.map getEntries
  =$= CC.concat
  =$= CC.filter hasDatExt
  where
    getEntries ar = catMaybes $ fmap (`findEntryByPath` ar) (filesInArchive ar)
    hasDatExt en = T.toUpper (fromMaybe "" (F.extension (entryPath en))) == "DAT"
    entryPath = F.fromText . T.pack . eRelativePath

askForPersonNameMode
  :: forall a m.
     (HasPersonName a, MonadResource m)
     =>
     Get a -> Consumer BS8.ByteString m PersonNameMode
askForPersonNameMode getFunc =
      CC.filterE (/= 10)        -- filter out spaces
  =$= CS.conduitGet getFunc
  =$  askForPersonNameModeLoop

askForPersonNameModeLoop
  :: forall a m.
     (HasPersonName a, MonadResource m)
     =>
     Consumer a m PersonNameMode
askForPersonNameModeLoop = do
  mEntity <- await
  case mEntity of
    Nothing     -> return NoInteractionName
    Just entity -> do
      liftIO $ putStrLn "Which of the two choices (A or B) looks better:"
      liftIO $ putStrLn $ "A: [" <> T.unpack (nameOneField entity) <> "]"
      liftIO $ putStrLn $ "B: [" <> T.unpack (nameThreeFields entity) <> "]"
      isNull <- CC.null
      let getCharLoop = do
            liftIO $ putStrLn $ "Type 'a' or 'b'"
              <> if isNull then ":" else " (or 'm' for more examples):"
            line <- liftIO getLine
            case line of
              "a" -> return OneFieldName
              "b" -> return ThreeFieldsName
              "m" -> if isNull then getCharLoop else askForPersonNameModeLoop
              _   -> getCharLoop
      getCharLoop

-- | Read a file from a file path or a zip entry and insert its contents into
-- the database. A new DB connection is started on every call to this functions,
-- thus for every file reference.
readFileRefIntoDb
  :: forall m.
     (MonadIO m, MonadCatch m, MonadBaseControl IO m)
     =>
     [String] -> ConnectionPool -> (FileRef, Maybe PersonNameMode) -> m ()
readFileRefIntoDb grantUsers pool (fRef, mMode) =
  case getAnyPersistEntity fRef of
    Just anyGetFunc ->
      handleAll (expWhen $ "inserting rows to " <> show fRef) $
      handle (expWhenParseError $ "reading file " <> show fRef) $
        liftIO $ flip runSqlPersistMPool pool $ do
          liftIO $ putStrLn $ "Inserting rows from " <> show fRef
          sourceFileRef fRef $$ sinkDatContentsToDb anyGetFunc mMode
          let tableName' = getTableName anyGetFunc
          liftIO $ putStrLn $ "Inserted "<> show fRef <>" to "<> show tableName'
          grantAccessAll tableName' grantUsers
    Nothing ->
      liftIO $ putStrLn $ "Warning: file " <> show fRef <> " not processed"

sinkDatContentsToDb
  :: (MonadIO m, MonadCatch m)
     =>
     AnyPersistEntityGetFunc
  -> Maybe PersonNameMode
  -> Consumer BS8.ByteString (ReaderT SqlBackend m) ()
sinkDatContentsToDb (MkAPEGF getFunc) _mode =
      CC.filterE (/= 10)            -- filter out spaces
  =$= CS.conduitGet getFunc
  =$= CL.sequence (CL.take 1000)    -- bulk insert in chunks of size 1000
  =$  CC.mapM_ insertMany_
sinkDatContentsToDb (MkAPEGF' getFunc) (Just mode) =
      CC.filterE (/= 10)            -- filter out spaces
  =$= CS.conduitGet (getFunc mode)
  =$= CL.sequence (CL.take 1000)    -- bulk insert in chunks of size 1000
  =$  CC.mapM_ insertMany_
sinkDatContentsToDb (MkAPEGF' _) Nothing =
  error "sinkDatContentsToDb: no PersonNameMode specified"

getTableName :: AnyPersistEntityGetFunc -> Text
getTableName = getTableName'
  where
    getTableName' :: AnyPersistEntityGetFunc -> Text
    getTableName' (MkAPEGF  getFunc) = getTableName'' getFunc
    getTableName' (MkAPEGF' getFunc) = getTableName'' $
                                       getFunc TryOneAndThreeFieldsName
    getTableName''
      :: forall a.
         (PersistEntity a, PersistEntityBackend a ~ SqlBackend)
         =>
         Get a -> Text
    -- The following call to head is never performed, so code works even for
    -- empty lists
    getTableName'' _getFunc = T.filter (/='"') $ tableName (head ([] :: [a]))

grantAccessAll
  :: forall m.
     (MonadIO m, MonadCatch m)
     =>
     Text -> [String] -> ReaderT SqlBackend m ()
grantAccessAll tableName' grantUsers =
  forM_ grantUsers $ \user' ->
    handleAll (expWhen ("granting access privileges for user " ++ user')) $
      grantAccess tableName' user'

-- | Grant access to 'table' for database user 'dbUser' using a raw PostgreSQL
-- query.
grantAccess
  :: forall m. MonadIO m
     =>
     Text -> String -> ReaderT SqlBackend m ()
grantAccess table dbUser
  = rawExecute ("GRANT SELECT ON " <> table <> " TO " <> T.pack dbUser) []

expWhen :: MonadIO m => String -> SomeException -> m ()
expWhen msg e = do
  let text = "\n\n" ++ "Caught when " ++ msg ++ " :" ++ show e ++ "\n\n"
  liftIO $ putStrLn text

expWhenParseError :: MonadIO m => String -> CS.ParseError -> m ()
expWhenParseError msg e@(CS.ParseError unconsumed' _ _) = do
  let e' = e {CS.unconsumed = BS8.take 1500 unconsumed'}
  let text = "\n\n" ++ "Caught when " ++ msg ++ " :" ++ show e' ++ "\n\n"
  liftIO $ putStrLn text
