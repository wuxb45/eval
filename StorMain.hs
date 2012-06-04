import Eval.DServ
import Eval.Storage
import System.Environment

main :: IO ()
main = do
  mbzk <- findDefaultZK
  case mbzk of
    Just zkinfo -> do
      (arg:_) <- getArgs
      case arg of
        "server" -> runStorSimpleServer zkinfo 7666 "/home/wuxb/tmp/storage"
        "client" -> runStorClientREPL zkinfo
        _ -> putStrLn "???"
    _ -> putStrLn "please write <host>:<port> into your ~/.zkrc"

