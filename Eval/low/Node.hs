-- Copyright 2012 Wu Xingbo
-- head {{{
{-# LANGUAGE ScopedTypeVariables, DeriveDataTypeable #-}
-- }}}

-- module export {{{
module Eval.Node (
  SockNode,
  SockHandler,
  directSockNode,
  startSockNode,
  stopSockNode,

  sendRequest,
  sendRequestSafe,
  sendRequestMaybe,

  putBSL,
  getBSL,
  putObject,
  getObject,) where
-- }}}

-- import {{{
import Prelude (($), (.), (/), (-), (+), (>),
                String, Int, fromIntegral, id, Float, fst,)

import Control.Applicative ((<$>))
import Control.Monad (Monad(..), void)
import Control.Concurrent (forkIO, yield, myThreadId, ThreadId,)
import Control.Exception (SomeException, Exception, IOException,
                          handle, bracket, catch, throwTo, catches,
                          Handler(..))
import qualified Data.ByteString.Lazy as BSL
import Data.Binary (Binary, encode, decode,)
import Data.Bool (Bool(..))

import Data.Maybe (Maybe(..), maybe,)
import Data.Typeable (Typeable(..),)
import Network (HostName, PortNumber(..), PortID(..), Socket(..), connectTo,
                withSocketsDo, accept, listenOn, sClose,)
import Network.Socket(getNameInfo, SockAddr(..),)
import System.Log.Logger (errorM, debugM, infoM, warningM, noticeM, )
import System.CPUTime (getCPUTime,)
import System.IO (IO (..), hGetLine, hFlush, hPutStrLn, Handle, hClose,
                  hSetBuffering, BufferMode(..), putStrLn,)
import System.Posix.Signals (installHandler, sigPIPE)
import qualified System.Posix.Signals as Sig
import Data.List ((++), words, head)
import Text.Read (Read(..), read)
import Text.Show (Show(..))
import Text.Printf (printf)
-- }}}

-- Logger {{{
moduleName :: String
moduleName = "Eval.Node"
-- }}}

-- NodeDownException {{{
-- used for close node
data NodeDownEx = NodeDownEx ThreadId
  deriving (Typeable, Show)

instance Exception NodeDownEx where
-- }}}

-- SockNode {{{
data SockNode
  = SockNode {
    snThreadId    :: !ThreadId,
    snHostName    :: !HostName,
    snPortNumber  :: !PortNumber
  } deriving (Show)

type SockHandler a = Handle -> IO a

directSockNode :: SockHandler () -> PortNumber -> IO ()
directSockNode reqHandler port = do
  mbss <- getSocket port
  case mbss of
    Just ss -> do
      hostname <- getHostName
      servThread reqHandler ss
    _ -> return ()

startSockNode :: SockHandler () -> PortNumber -> IO (Maybe SockNode)
startSockNode reqHandler port = do
  mbss <- getSocket port
  case mbss of
    Just ss -> do
      hostname <- getHostName
      tid <- forkIO $ servThread reqHandler ss
      return (Just $ SockNode tid hostname port)
    _ -> return Nothing

servThread :: SockHandler () -> Socket -> IO ()
servThread f ss = servWith f ss `catches` exHandlers
  where
  exHandlers :: [Handler ()]
  exHandlers = [Handler $ closeHandler ss, Handler defaultHandler]
  defaultHandler :: SomeException -> IO ()
  defaultHandler e = do
    putStrLn $ "ERROR - socket closed with exception: " ++ show e
    sClose ss
  closeHandler :: Socket -> NodeDownEx -> IO ()
  closeHandler so (NodeDownEx tid) = do
    sClose ss
    putStrLn $ "socket normally closed by thread " ++ show tid

stopSockNode :: SockNode -> IO ()
stopSockNode ss = do
  me <- myThreadId
  throwTo (snThreadId ss) (NodeDownEx me)
  yield

getHostName :: IO HostName
getHostName = do
    mbhost <- fst <$> getNameInfo [] True False (SockAddrUnix "localhost")
    return $ maybe "localhost" id mbhost

getSocket :: PortNumber -> IO (Maybe Socket)
getSocket port = catch (Just <$> listenOn (PortNumber port)) exHandler
  where
    exHandler (e :: SomeException) = do
      putStrLn $ "listenOn failed. " ++ show e
      return Nothing

servWith :: SockHandler () -> Socket -> IO ()
servWith f socket = do
  (hdl, _, _) <- accept socket
  void $ forkIO $ servRequest f hdl
  servWith f socket

servRequest :: SockHandler () -> Handle -> IO ()
servRequest f conn = bracket (return conn) hClose serv
  where
    exHandler (e :: SomeException) = do
      errorM moduleName $ "servRequest: " ++ show e
    serv hdl = do
      hSetBuffering hdl $ BlockBuffering Nothing -- use buffer
      handle exHandler $ f hdl
-- }}}

-- client {{{
sendRequest :: SockHandler a -> HostName -> PortNumber -> IO a
sendRequest f n p = 
  withSocketsDo $ do
    _ <- installHandler sigPIPE Sig.Ignore Nothing
    bracket (connectTo n (PortNumber p)) (hClose) (send)
  where    
    send hdl = do
      hSetBuffering hdl $ BlockBuffering Nothing
      f hdl 

sendRequestSafe :: SockHandler a -> a -> HostName -> PortNumber -> IO a
sendRequestSafe f d n p = catch (sendRequest f n p) exHandler
  where
    exHandler (e :: SomeException) = do
      debugM moduleName $ "sendRequestSafe" ++ show e 
      return d

sendRequestMaybe :: SockHandler a -> HostName -> PortNumber -> IO (Maybe a)
sendRequestMaybe f n p = catch (Just <$> sendRequest f n p) exHandler
  where
    exHandler (e :: IOException) = do
      debugM moduleName $ "sendRequestMaybe: " ++ show e 
      return Nothing
-- }}}

-- put/get ByteString {{{
-- | Puts a bytestring to a handle. But to make the reading easier, we write
--   the length of the data as a message-header to the handle, too. 
putBSL :: BSL.ByteString -> Handle -> IO ()
putBSL msg hdl = do
  handle exHandler $ do
    hPutStrLn hdl $ (show $ BSL.length msg) ++ " "
    BSL.hPut hdl msg
    hFlush hdl
  where
    exHandler (e :: SomeException) = errorM moduleName $ "putMessage: " ++ show msg

-- | Reads data from a stream. We define, that the first line of the message
--   is the message header which tells us how much bytes we have to read.
getBSL :: Handle -> IO (BSL.ByteString)
getBSL hdl = do
  line <- hGetLine hdl
  bs <- BSL.hGet hdl (read $ line :: Int)
  return bs
-- }}}

-- put/get Object on System.IO.Handle {{{
putObject :: Binary a => a -> Handle -> IO ()
putObject obj h = putBSL (encode obj) h 

getObject :: Binary a => Handle -> IO a
getObject h = decode <$> getBSL h

-- }}}

-- vim:fdm=marker
