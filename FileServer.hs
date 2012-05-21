{-# LANGUAGE ScopedTypeVariables #-}
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
import Control.Applicative
import Data.Maybe
import System.IO
import System.Environment
import Data.Int
import System.Posix.Files
import Control.Monad

import Eval.DServ

zkinfo = ZKInfo "dell:2181"

pipeAll :: Int -> Handle -> Handle -> IO ()
pipeAll remains from to = do
  let thistime = if remains > lim then lim else remains
  bs <- BS.hGet from thistime
  BS.hPut to bs
  --putStrLn $ show ("pipe once:", show thistime, BS.length bs)
  let remains' = remains - thistime
  when (remains' /= 0) $ pipeAll remains' from to
  where lim = 0x100000

serverH :: IOHandler
serverH remoteH = do
  mbfilename <- getObject remoteH
  putStrLn $ "get filename: " ++ show mbfilename
  case mbfilename of
    Just (filename :: String) -> do
      (size :: Int64) <- (fromIntegral . fileSize) <$> getFileStatus filename
      putStrLn $ show ("user request:", filename, "size:", size)
      putObject remoteH size
      BSL.readFile filename >>= BSL.hPut remoteH
      putObject remoteH True
    _ -> do
      putStrLn "error request"

service = DService "rawFile" serverH Nothing

clientH :: String -> Handle -> IO Int64
clientH filename remoteH = do
  putObject remoteH filename
  (mbsize :: Maybe Int64) <- getObject remoteH
  putStrLn $ "get size:" ++ show mbsize
  case mbsize of
    Just size -> do
      putStrLn $ show ("request for:", filename, "file size:", size)
      fileH <- openBinaryFile "/home/wuxb/get1" WriteMode
      putStrLn "open recv file, ok"
      hSetBuffering fileH $ BlockBuffering Nothing
      hSetBuffering remoteH $ BlockBuffering Nothing
      putStrLn "setBuffering, ok"
      --pipeAll (fromIntegral size) remoteH fileH
      bs <- BSL.hGet remoteH $ fromIntegral size
      BSL.hPut fileH bs
      (mbComplete :: Maybe Bool) <- getObject remoteH
      putStrLn $ "complete? " ++ show mbComplete
      return size
    _ -> do
      putStrLn "no size?"
      error "no size"

main :: IO ()
main = do
  (arg:rest) <- getArgs
  case arg of
    "server" -> do
      ser <- forkServer zkinfo service 8899
      putStrLn $ show ser
      void getLine
      closeServer $ fromJust ser
      return ()
    "client" -> do
      mbList <- listServer zkinfo
      case mbList of
        Just (si:_) -> do
          result <- accessServer si (clientH $ head rest)
          putStrLn $ "result: " ++ show result
        _ -> putStrLn "no service"
      return ()
    _ -> return ()

