{-# LANGUAGE ScopedTypeVariables #-}
import Prelude hiding (catch)
import Eval.DServ
import Eval.Storage
import System.Environment
import Text.Printf
import Control.Applicative
import System.IO
import Control.Exception

main :: IO ()
main = do
  commonInitial
  (arg:rest) <- getArgs
  case arg of
    "server" -> runServer rest
    "client" -> runClient
    "dummy" -> runDummyCmd rest
    _ -> putStrLn "???"

zkinfo :: ZKInfo
zkinfo = ZKInfo "dell:2181"

runServer :: [String] -> IO ()
runServer args = do
  let serverport = case args of
               (port:_) -> read port
               _ -> 7666
  service <- makeDSService "/home/wuxb/tmp/storage"
  mbsd <- forkServer zkinfo service serverport
  putStrLn $ "start, server data: " ++ show mbsd
  case mbsd of
    Just sd -> waitCloseSignal sd
    _ -> return ()

runClient :: IO ()
runClient = do
  lst <- maybe [] id <$> listServer zkinfo
  putStrLn $ "==============================================================="
  putStrLn $ "==============================================================="
  putStrLn $ "current server list (" ++ show (length lst) ++ ") :"
  mapM_ (putStrLn . show) $ zip [(0 :: Integer) ..] lst
  printf "DStorage > " >> hFlush stdout
  cmd <- words <$> (getLine `catch` aHandler "quit")
  case cmd of
    ["quit"] -> putStrLn "bye!" >> hFlush stdout >> return ()
    (ix:cmd') -> do
      if ix == "*"
      then mapM_ (\si -> accessAndShow si cmd') lst
      else do
        if all (`elem` "0123456789") ix
        then accessAndShow (lst !! (read ix)) cmd'
        else accessAndShow (lst !! 0) cmd
      runClient
    _ -> do
      accessAndShow (lst !! 0) cmd
      runClient

runDummyCmd :: [String] -> IO ()
runDummyCmd args = do
  lst <- maybe [] id <$> listServer zkinfo
  putStrLn $ "server list:"
  mapM_ (putStrLn . show) $ zip [(0 :: Integer)..] lst
  case lst of
    (a:_) -> accessAndShow a args
    _ -> putStrLn "no online server"


accessAndShow :: DServerInfo -> [String] -> IO ()
accessAndShow si args = do
  result <- accessServer si (clientCmdH args)
  case result of
    Just (Left ok) -> putStrLn $ "result: " ++ show ok
    Just (Right lst) -> putStrLn "result list: " >> mapM_ putStrLn lst
    _ -> putStrLn "accessServer failed"
