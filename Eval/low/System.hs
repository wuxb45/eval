module System
  ( ignoreSignal,
    sigABRT, sigALRM, sigBUS, sigCHLD, sigCONT,
    sigFPE, sigHUP, sigILL, sigINT, sigKILL,
    sigPIPE, sigQUIT, sigSEGV, sigSTOP, sigTERM,
    sigTSTP, sigTTIN, sigTTOU, sigUSR1, sigUSR2,
    sigPOLL, sigPROF, sigSYS, sigTRAP, sigURG,
    sigVTALRM, sigXCPU, sigXFSZ,
  ) where

import System.Posix.Signals
import Control.Monad (void)


ignoreSignal :: Signal -> IO ()
ignoreSignal sig = void $ installHandler sig Ignore Nothing
