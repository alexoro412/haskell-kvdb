{-# LANGUAGE OverloadedStrings #-}

module KVDB.Storage
  (Storage (..),
  DB(..),
  Entry(..),
  ErrorCode(..),
  Result(..),
  KVDB.Storage.empty,
  get,
  set)
where

import Data.ByteString
import Control.Concurrent.STM
import Data.Map.Strict
-- import qualified Text.Show.ByteString as BSS


-- data Type = Raw | Hash

--                (Type,       Value)
data Entry = Raw ByteString | Hash (Map ByteString ByteString)

type Storage = Map ByteString (TVar Entry)
type DB = TVar Storage
data ErrorCode = Nil | TypeMismatch deriving Show
data Result = Error ErrorCode | Ok ByteString deriving Show

-- TODO
-- recognize uppercase commands
-- del, exists, hdel, hget, hset, clear

empty :: STM DB
empty = newTVar (Data.Map.Strict.empty :: Storage)

set :: DB -> ByteString -> ByteString -> STM Result
set db k v = do
  db_map <- readTVar db
  case Data.Map.Strict.lookup k db_map of
    Nothing -> do
      entry <- newTVar $ Raw v
      writeTVar db (insert k entry db_map)
      return $ Ok ""
    Just entry -> do
      derefedEntry <- readTVar entry
      case derefedEntry of
        Hash _ -> return $ Error TypeMismatch
        Raw _ -> do
          entry <- newTVar $ Raw v
          writeTVar db (insert k entry db_map)
          return $ Ok ""

get :: DB -> ByteString -> STM Result
get db k = do
  db_map <- readTVar db
  case Data.Map.Strict.lookup k db_map of
    Nothing -> do
      return $ Error Nil
    Just entry -> do
      derefedEntry <- readTVar entry
      case derefedEntry of
        Hash _ -> return $ Error TypeMismatch
        Raw b -> do
          return $ Ok b
