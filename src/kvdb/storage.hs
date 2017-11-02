{-# LANGUAGE OverloadedStrings #-}

module KVDB.Storage
  (Storage (..),
  DB(..),
  Entry(..),
  ErrorCode(..),
  Result(..),
  KVDB.Storage.empty,
  get,
  set,
  exists,
  del,
  hset,
  hget,
  hdel,
  clear)
where

import Data.ByteString hiding (pack, foldl, length, zip)
import Data.ByteString.Char8 (pack)
import Control.Concurrent.STM
import Data.Map.Strict hiding (foldl)
-- import qualified Text.Show.ByteString as BSS

--                (Type,       Value)
data Entry = Raw ByteString | Hash (Map ByteString ByteString)
data Type = RawType | HashType

type Storage = Map ByteString (Type, TVar Entry)
type DB = TVar Storage
data ErrorCode = Nil | TypeMismatch | Syntax deriving Show
data Result = Error ErrorCode | Ok ByteString deriving Show

-- TODO
-- recognize uppercase commands
-- hdel, hget, hset, clear

empty :: STM DB
empty = newTVar (Data.Map.Strict.empty :: Storage)

set :: DB -> ByteString -> ByteString -> STM Result
set db k v = do
  db_map <- readTVar db
  case Data.Map.Strict.lookup k db_map of
    Nothing -> do
      entry <- newTVar $ Raw v
      writeTVar db (insert k (RawType, entry) db_map)
      return $ Ok ""
    Just (RawType, _) -> do
      entry <- newTVar $ Raw v
      writeTVar db (insert k (RawType, entry) db_map)
      return $ Ok ""
    Just (HashType, _) -> return $ Error TypeMismatch

get :: DB -> ByteString -> STM Result
get db k = do
  db_map <- readTVar db
  case Data.Map.Strict.lookup k db_map of
    Nothing -> do
      return $ Error Nil
    Just (HashType, _) -> return $ Error TypeMismatch
    Just (RawType, entry) -> do
      Raw b <- readTVar entry
      return $ Ok b

exists :: DB -> [ByteString] -> STM Result
exists db ks = do
  db_map <- readTVar db
  let num = foldl ((\acc k -> if k `member` db_map then (acc + 1) else acc) :: Int -> ByteString -> Int) 0 ks
  return $ Ok $ pack $ show num
  -- fmap (Ok . pack) $ exists_rec db_map ks 0

del :: DB -> [ByteString] -> STM Result
del db ks = do
  db_map <- readTVar db
  let (num, new) = foldl (\(number, storage) key -> if key `member` db_map then (number + 1, delete key storage) else (number, storage)) (0, db_map) ks
  writeTVar db new
  return $ Ok $ pack $ show num

hdel :: DB -> ByteString -> [ByteString] -> STM Result
hdel db hkey ks = do
  db_map <- readTVar db
  case Data.Map.Strict.lookup hkey db_map of
    Nothing -> return $ Ok "0"
    Just (HashType, tvar) -> do
      Hash hash <- readTVar tvar
      let diffMap = fromList $ zip ks [1..]
          newMap = hash `difference` diffMap
      writeTVar tvar (Hash newMap)
      return $ Ok $ pack $ show (size hash - size newMap)
    Just _ -> return $ Error TypeMismatch

hget :: DB -> ByteString -> ByteString -> STM Result
hget db hkey k = do
  db_map <- readTVar db
  case Data.Map.Strict.lookup hkey db_map of
    Nothing -> return $ Error Nil
    Just (HashType, tvar) -> do
      Hash hash <- readTVar tvar
      case Data.Map.Strict.lookup k hash of
        Nothing -> return $ Error Nil
        Just v -> return $ Ok v
    Just _ -> return $ Error TypeMismatch

listToAssoc :: [a] -> [(a,a)]
listToAssoc [] = []
listToAssoc (a:b:xs) = (a,b):(listToAssoc xs)

hset :: DB -> [ByteString] -> STM Result
hset db (hkey:kvs) = do
  if even $ length kvs then do
    db_map <- readTVar db
    let addition = fromList $ listToAssoc kvs
    case Data.Map.Strict.lookup hkey db_map of
      Just (HashType, tvar) -> do
        Hash hash <- readTVar tvar
        writeTVar tvar $ Hash (hash `union` addition)
        return $ Ok ""
        -- writeTVar db (insert hkey (Hash (hash `union` addition)) db_map)
      -- Just x -> return $ Error TypeMismatch
      Just _ -> return $ Error TypeMismatch
      Nothing -> do
        hash <- newTVar (Hash addition)
        writeTVar db (insert hkey (HashType, hash) db_map)
        return $ Ok ""
    return $ Ok ""
  else
    return $ Error Syntax

clear :: DB -> STM Result
clear database = do
  writeTVar database Data.Map.Strict.empty
  return $ Ok ""


-- exists_rec :: Map -> [ByteString] -> STM Int
-- exists_rec db [] acc = do
--   return acc
-- exists_rec db k:[] = do
--   existing <- member k db
--   if existing then
--     return $ acc + 1
--   else
--     return acc
-- exists_rec db k:ks = do
--   let num = foldl (\acc k -> ) 0 ks
--   return $ Ok (pack num)
--
-- single_exists ::
