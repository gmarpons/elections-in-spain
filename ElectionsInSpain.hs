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
import           Data.Conduit.Combinators          hiding (concat, print)
import           Data.Conduit.Serialization.Binary
import           Data.String
import           Data.Text                         (Text)
import qualified Data.Text                         as T
import           Database.Persist                  hiding (get)
import           Database.Persist.Postgresql       hiding (get)
import           Database.Persist.TH
import           Filesystem.Path.CurrentOS         ((</>))
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
    Candidaturas
      id_candidatura                Text
      codigo_candidatura            Int
      tipoEleccion                  Int
      ano                           Int
      mes                           Int
      siglas                        Text sqltype=varchar(50)
      denominacion                  Text sqltype=varchar(150)
      codigo_candidatura_provincial Int
      codigo_candidatura_autonomico Int
      codigo_candidatura_nacional   Int
      -- Use a unique column instead of a primary key because PKEY + repsert
      -- fails (probably a bug)
      UniqueIdCandidatura id_candidatura
      deriving Show
  |]

skipToNextLineAfter :: Get a -> Get a
skipToNextLineAfter item = item <* skip 1

getCandidatura :: Get Candidaturas
getCandidatura =
  mk Candidaturas
  <$> getTipoEleccion
  <*> getAno
  <*> getMes
  <*> getCodigoCandidatura
  <*> getSiglas
  <*> getDenominacion
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getCodigoCandidatura)
  <*> (snd <$> getCodigoCandidatura)
  where
    mk f (t, t') (a, a') (m, m') (c, c') = f (T.concat [t, a, m, c]) c' t' a' m'

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

-- | Given a number of bytes to read, gets and @Int@ (into the Get monad) both
-- as a number and as Text. WARNING: partial function, uses @read@, so if fails
-- if some of the obtained bytes is not a decimal digit.
getInt :: Int -> Get (Text, Int)
getInt n = (T.pack &&& read) . B8.unpack <$> getByteString n

-- | Given a number of bytes to read, gets (into the Get monad) the end-stripped
-- @Text@ represented by those bytes.
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
      runMigration migrateAll
      let fileName = b </> "03041105.DAT"
      sourceFile fileName $= conduitGet (skipToNextLineAfter getCandidatura) $$ sinkToDb
      let tables = ["candidaturas"]
      forM_ [(u_, t) | u_ <- g, t <- tables] $ \(user_, table) ->
        handleAll (expWhen ("granting access privileges for user " ++ user_)) $
          grantAccess table user_
  where
    options' = info (helper <*> options) helpMessage
    pgConnOpts d u p = B8.pack $ concat [d, u, p]
    expWhen :: (MonadIO m, MonadCatch m) => String -> SomeException -> m ()
    expWhen msg e = do
      liftIO $ put2Ln >> putStr "Caught when " >> putStr msg >> putStr " :"
      liftIO $ print e >> put2Ln
    put2Ln = putStrLn "" >> putStrLn ""

grantAccess :: (MonadBaseControl IO m, MonadLogger m, MonadIO m)
               =>
               Text -> String -> ReaderT SqlBackend m ()
grantAccess t u = rawExecute (T.concat ["GRANT SELECT ON ", t," TO ", u']) []
  where
    u' = T.pack u

sinkToDb :: (MonadResource m) => Sink Candidaturas (ReaderT SqlBackend m) ()
sinkToDb = awaitForever $ \c -> lift $ upsert c []
