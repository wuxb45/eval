import System.Environment
import Eval.DServ
import Eval.DSet

zkinfo :: ZKInfo
zkinfo = ZKInfo "dell:2181"

main :: IO ()
main = do
  (a:_) <- getArgs
  case a of
    "server" -> runDSetSimpleServer zkinfo 7610 "/home/wuxb/tmp/dset"
    "client" -> runDSetClientREPL zkinfo
    _ -> putStrLn "??"
