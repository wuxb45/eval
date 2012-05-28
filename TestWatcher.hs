import Eval.DServ
import qualified Zookeeper as Zoo
import Control.Concurrent
import Control.Concurrent.MVar

zkinfo = ZKInfo "dell:2181"
main :: IO ()
main = do
  commonInitial
  _ <- forkChildrenWatcher zkinfo "/d" $ putStrLn . show
  -- just wait..
  sequence_ $ repeat $ threadDelay 100000
  
