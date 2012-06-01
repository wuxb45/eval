import Eval.DServ
import Control.Applicative

main :: IO ()
main = do
  commonInitial
  mbzk <- findDefaultZK
  case mbzk of
    Just zkinfo -> do
      lst <- maybe [] id <$> listServer zkinfo
      putStrLn $ "online service (" ++ show (length lst) ++ "):"
      mapM_ (putStrLn . show) lst
    _ -> putStrLn "no zookeeper info, check ~/.zkrc"
