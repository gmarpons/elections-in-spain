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
import           Control.Lens
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Control.Monad.Logger
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.Reader
import qualified Data.ByteString.Char8       as B8 (pack)
import           Data.Char
import qualified Data.Map.Strict             as M
import           Data.Maybe
import qualified Data.Set                    as S
import           Data.String
import           Data.Text                   (Text)
import qualified Data.Text                   as T
import           Data.Time.Clock
import           Database.Persist
import           Database.Persist.Postgresql
import           Database.Persist.TH
import qualified Filesystem.Path.CurrentOS   as F
import           Options.Applicative


-- |
-- = DB schema definition

share
  [ mkPersist sqlSettings { mpsPrefixFields   = True
                          , mpsGeneric        = False
                          , mpsGenerateLenses = True
                          }
  , mkMigrate "migrateAll"
  ]
  [persistLowerCase|
  |]


-- |
-- = Command line options

data Options
  = Options
    { file               :: F.FilePath
    , dbname             :: String
    , user               :: String
    , password           :: String
    , usersToGrantAccess :: [String]
    }

options :: Parser Options
options = Options
  <$> option (fromString <$> str)
  ( long "files"
    <> short 'f'
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
  <> header "bcnodata2db - Relational-ize OData from http://opendata.bcn.cat"


-- | = Entry point and auxiliary functions. All SQL commands are run in a single
-- transaction. This has some advantages (performance, if something fails no
-- data is destroyed) and one big drawback: if a collection gets broken, we
-- can't update the other collections.
main :: IO ()
main = execParser options' >>= \(Options f d u p g) -> do
  runNoLoggingT $ withPostgresqlPool (pqConnOpts d u p) 10 $ \pool ->
    liftIO $ flip runSqlPersistMPool pool $ do
      runMigration migrateAll
      forM_ g $ \user_ ->
        handleAll (expWhen ("granting access privileges for user " ++ user_)) $
          grantAccess "fonts_de_dades" user_
  where
    options' = info (helper <*> options) helpMessage
    pqConnOpts d u p = B8.pack $ concat [d, u, p]
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
