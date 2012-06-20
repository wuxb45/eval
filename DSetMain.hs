import System.Environment
import Eval.DServ
import Eval.DSet

main :: IO ()
main = do
  mbzk <- findDefaultZK
  case mbzk of
    Just zkinfo -> do
      (a:b) <- getArgs
      case a of
        "server" -> runDSetSimpleServer zkinfo 7610 "/home/wuxb/tmp/dset"
        "client" -> runDSetClientREPL zkinfo
        "test1" -> testAddN zkinfo (head b)
        _ -> putStrLn "??!!"
    _ -> putStrLn "please write <host>:<port> into your ~/.zkrc"
