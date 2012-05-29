import System.Environment
import Eval.DServ
import Eval.Storage
import EVal.DSet

zkinfo = ZKInfo "dell:2181"



main :: IO ()
main = do
  (n:f:_) <- getArgs
  re <- uploadSingleton zkinfo n f
  putStrLn $ show re
