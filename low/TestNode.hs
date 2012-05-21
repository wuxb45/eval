{-# LANGUAGE ScopedTypeVariables #-}
import Eval.Node
import System.Environment
import Data.Binary
import Control.Applicative ((<$>), (<*>))
import Network (PortNumber, HostName)
import System.IO (getLine)

data ADDExpr = ADDExpr Integer Integer
data ADDResult = ADDResult Integer

evalExpr :: ADDExpr -> ADDResult
evalExpr (ADDExpr a b) = ADDResult (a + b)

instance Show ADDExpr where
  show (ADDExpr a b) = "(" ++ show a ++ " + " ++ show b ++ ")"
instance Binary ADDExpr where
  put (ADDExpr a b) = putWord8 128 >> put a >> put b
  get = do
    tag <- getWord8
    case tag of
      128 -> ADDExpr <$> get <*> get
      _ -> error "bad ADDExpr"
instance Show ADDResult where
  show (ADDResult x) = "(" ++ show x ++ ")"
instance Binary ADDResult where
  put (ADDResult x) = putWord8 64 >> put x
  get = do
    tag <- getWord8
    case tag of
      64 -> ADDResult <$> get

testPort :: PortNumber
testPort = fromIntegral 1313

main :: IO ()
main = do
  (a:rest) <- getArgs
  case a of
    "server" -> testServer
    "client" -> case rest of
                  (h:x:y:_) -> testClient h x y
                  _ -> putStrLn "client need 3 args: hostname a b"
    _ -> putStrLn "??"

servHandler :: SockHandler ()
servHandler h = do
  (expr :: ADDExpr) <- getObject h
  putStrLn $ "get request" ++ show expr
  putObject (evalExpr expr) h
  return ()

testServer :: IO ()
testServer = directSockNode servHandler testPort

testClient :: HostName -> String -> String -> IO ()
testClient host x y = sendRequest sHandler host testPort
  where
    obj = ADDExpr (read x) (read y)
    sHandler :: SockHandler ()
    sHandler h = do
      putObject obj h
      (r :: ADDResult) <- getObject h
      putStrLn $ "result is" ++ show r
