{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
import Eval.DServ
import Control.Monad (void)
import Data.Maybe (fromJust)
import GHC.Generics
import System.IO (Handle)
import Data.Serialize
import Control.Concurrent.MVar
import System.Environment

data DummyData = DD String Integer deriving (Generic, Show)
instance Serialize DummyData where

main :: IO ()
main = do
  (arg:_) <- getArgs
  case arg of
    "server" -> doServer
    "client" -> doClient
    _ -> putStrLn "unknown command"

zkInfo :: ZKInfo
zkInfo = ZKInfo "localhost:2181"

makeService :: IO DService
makeService = do
  ref <- newMVar (0 :: Integer)
  return $ DService "dummy" (myHandler ref) Nothing
  where
    myHandler :: MVar Integer -> IOHandler
    myHandler mvar h = do
      i <- takeMVar mvar
      putMVar mvar (i + 1)
      putObject h $ DD "Dummy Counter" i

clientHandler :: Handle -> IO DummyData
clientHandler h = do
  mbdd <- getObject h
  return $ fromJust mbdd

doServer :: IO ()
doServer = do
  service <- makeService
  mbServerData <- forkServer zkInfo service 7788
  putStrLn $ "server data: " ++ show mbServerData
  void getLine
  closeServer $ fromJust mbServerData

doClient :: IO ()
doClient = do
  mbList <- listServer zkInfo
  putStrLn $ "server list: " ++ show mbList
  case mbList of
    Just (a:_) -> accessServer a clientHandler >>= (putStrLn . show)
    _ -> putStrLn "xxx"

