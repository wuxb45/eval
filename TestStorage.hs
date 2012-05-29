import Eval.DServ
import Eval.Storage
import System.Environment

zkinfo :: ZKInfo
zkinfo = ZKInfo "dell:2181"

main :: IO ()
main = do
  (arg:_) <- getArgs
  case arg of
    "server" -> runStorSimpleServer zkinfo 7666 "/home/wuxb/tmp/storage"
    "client" -> runStorClientREPL zkinfo
    _ -> putStrLn "???"


