{-# LANGUAGE ScopedTypeVariables #-}
import Eval.DServ
import Eval.Storage
import System.Environment

main :: IO ()
main = do
  (arg:rest) <- getArgs
  case arg of
    "server" -> runServer
    "client" -> runClient rest
    _ -> putStrLn "<client> or <server>?"

zkinfo :: ZKInfo
zkinfo = ZKInfo "dell:2181"

runServer :: IO ()
runServer = do
  serv <- makeDSService "/home/wuxb/tmp/storage"
  mbsd <- forkServer zkinfo serv 7666
  putStrLn $ "start, server data: " ++ show mbsd
  case mbsd of
    Just sd -> waitCloseSignal sd
    _ -> return ()

runClient :: [String] -> IO ()
runClient args = do
  commonInitial
  mbList <- listServer zkinfo
  putStrLn $ "server list: " ++ show mbList
  case mbList of
    Just (a:_) -> do
      result <- accessServer a (clientCmdH args)
      result' <- accessServer a (clientCmdH ["backup"])
      case result of
        Just (Left ok) -> putStrLn $ "result: " ++ show ok
        Just (Right lst) -> putStrLn "resule list: " >> mapM_ putStrLn lst
        _ -> putStrLn "accessServer failed"
      case result' of
        Just (Left ok) -> putStrLn $ "backup " ++ show ok
        Just _ -> putStrLn "backup???"
        _ -> putStrLn "backup: accessServer failed"
    _ -> putStrLn "no server!!"
