import Eval.DServ
import Eval.Storage
import System.Environment

zkinfo :: ZKInfo
zkinfo = ZKInfo "dell:2181"

main :: IO ()
main = do
  (arg:_) <- getArgs
  case arg of
    "server" -> runSimpleServer zkinfo 7666 "/home/wuxb/tmp/storage"
    "client" -> runClientREPL zkinfo
    _ -> putStrLn "???"


