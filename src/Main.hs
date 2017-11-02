{-# LANGUAGE OverloadedStrings #-}

module Main where

import Network.Socket
import System.IO hiding (hPutStrLn, hGetLine, putStrLn)
import Control.Concurrent
import Control.Exception
import Control.Monad (when)
import Control.Monad.Fix (fix)
import Control.Concurrent.STM
import Prelude hiding (init, words, putStrLn)
import Data.ByteString hiding (pack, hPutStrLn, hGetLine, putStrLn)
import Data.ByteString.Char8 (words, pack, hPutStrLn, hGetLine, putStrLn)
import qualified Data.ByteString.Char8 as B8
import Data.Char (toLower)
import qualified KVDB.Storage as S

type Msg = (Int, String)

main :: IO ()
main = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 4040 iNADDR_ANY)
  listen sock 50

  -- create db TVar
  database <- atomically $ S.empty
  putStrLn "Listening on port 4040"
  mainLoop sock database 0
  return ()
  -- mainLoop sock db 0

mainLoop :: Socket -> S.DB -> Int -> IO ()
mainLoop sock db num = do
  conn <- accept sock
  putStrLn $ pack $ "starting thread " ++ (show num)
  forkIO $ runConn conn db num
  mainLoop sock db (num + 1)


runConn :: (Socket, SockAddr) -> S.DB -> Int -> IO ()
runConn (sock, _) database num = do
  -- putStrLn "Client connected"
  -- let broadcast msg = writeChan chan (msgNum, msg)

  -- sets up a handle
  hdl <- socketToHandle sock ReadWriteMode
  -- disables buffering
  hSetBuffering hdl NoBuffering

  handle (\(SomeException _) -> return ()) $ fix $ \loop -> do
    cmd:args <- fmap words $ hGetLine hdl -- init removes the newline
    -- putStrLn "got data"
    -- putStrLn $ pack $ show (cmd:args)
    resp <- runCmd database cmd args
    hPutStrLn hdl resp
    -- putStrLn "also here"
    loop
    -- killThread reader
  hClose hdl
  putStrLn $ pack $ "finish thread " ++ (show num)

errorToByteString :: S.ErrorCode -> ByteString
errorToByteString e = pack ("Error: " ++ show e)

runCmd :: S.DB -> ByteString -> [ByteString] -> IO ByteString
runCmd database cmd args = do
  result <- case (B8.map toLower cmd):args of
    "async":new_cmd:rest -> do
      forkIO $ runCmd database new_cmd rest >> return ()
      return $ S.Ok ""
    ["get",k] -> atomically $ S.get database k
    ["set",k,v] -> atomically $ S.set database k v
    "exists":ks -> atomically $ S.exists database ks
    "del":ks -> atomically $ S.del database ks
    "hset":hkvs -> atomically $ S.hset database hkvs
    "hget":hkey:[k] -> atomically $ S.hget database hkey k
    "hdel":hkey:ks -> atomically $ S.hdel database hkey ks
    "clear":[] -> atomically $ S.clear database
    _ -> return $ S.Error S.Syntax
  case result of
    S.Error e -> return $ errorToByteString e
    S.Ok v -> return v
