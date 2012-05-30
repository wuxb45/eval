import Eval.DServ
import Control.Applicative

main :: IO ()
main = do
  commonInitial
  lst <- maybe [] id <$> listServer (ZKInfo "dell:2181")
  mapM_ (putStrLn . show) lst
  
