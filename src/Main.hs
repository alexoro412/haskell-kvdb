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
-- import Text.Show.ByteString (show)
import qualified KVDB.Storage as S

type Msg = (Int, String)

main :: IO ()
main = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet 4242 iNADDR_ANY)
  listen sock 2

  -- create db TVar
  database <- atomically $ S.empty
  putStrLn "Listening on port 4242"
  mainLoop sock database
  -- this channel is never used, so a loop is added
  -- that throws away all data
  -- prevents memory leak
  -- chan <- newChan
  -- _ <- forkIO $ fix $ \loop -> do
  --   (_, _) <- readChan chan
  --   loop
  return ()
  -- mainLoop sock db 0

mainLoop :: Socket -> S.DB -> IO ()
mainLoop sock db = do
  conn <- accept sock
  forkIO $ runConn conn db
  mainLoop sock db
  -- msgNum is a unique id for each client
  -- conn <- accept sock
  -- forkIO $ runConn conn chan msgNum
  -- mainLoop sock chan $! msgNum + 1
  -- $ vs $!

runConn :: (Socket, SockAddr) -> S.DB -> IO ()
runConn (sock, _) database = do
  -- let broadcast msg = writeChan chan (msgNum, msg)

  -- sets up a handle
  hdl <- socketToHandle sock ReadWriteMode
  -- disables buffering
  hSetBuffering hdl NoBuffering

  handle (\(SomeException _) -> return ()) $ fix $ \loop -> do
    line <- fmap (words . init) $ hGetLine hdl -- init removes the newline
    case line of
      "quit":_ -> hPutStrLn hdl "Bye!"
      -- >> makes loop happen after broadcast
      ["get",k] -> do
        resp <- atomically $ S.get database k
        case resp of
          S.Error e -> hPutStrLn hdl $ pack ("ERR " ++ show e)
          S.Ok v -> hPutStrLn hdl v
        loop
      ["set",k,v] -> do
        resp <- atomically $ S.set database k v
        case resp of
          S.Error e -> hPutStrLn hdl $ pack ("ERR " ++ show e)
          S.Ok v -> hPutStrLn hdl v
        loop
      _ -> hPutStrLn hdl ("git rekt") >> loop

    -- killThread reader
    hClose hdl
  -- hPutStrLn hdl "Name: "
  -- init is used to cut off the newline
  -- name <- fmap init $ hGetLine hdl
  -- broadcast ("--> " ++ name ++ " entered chat.")
  -- hPutStrLn hdl ("Welcome, " ++ name ++ "!")

  -- creates duplicate channel
  -- channels are linked
  -- data written to one is available to all

  -- reads data off of channel
  -- and sends to client
  -- fix basically makes an infinite loop
  -- reader <- forkIO $ fix $ \loop -> do
  --   (nextNum, line) <- readChan commLine
  --   -- don't broadcast back to user
  --   when (msgNum /= nextNum) $ hPutStrLn hdl line
  --   loop

  -- Waits for an exception :D
  --
  -- handle (\(SomeException _) -> return ()) $ fix $ \loop -> do
  --   line <- fmap init $ hGetLine hdl -- init removes the newline
  --   case line of
  --     "quit" -> hPutStrLn hdl "Bye!"
  --     -- >> makes loop happen after broadcast
  --     _ -> broadcast (name ++ ": " ++ line) >> loop

  -- once thread closed, close the reader and the handle
