-- Copyright 2011 Wu Xingbo
-- LANGUAGE {{{
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
-- }}}

-- module export {{{
module Eval.DServ (
  aHandler, ioaHandler,
  AHandler, IOHandler,
  DServerInfo(..), DService(..), DServerData(..), ZKInfo(..),
  commonInitial,
  forkServer, closeServer,
  waitCloseCmd, waitCloseSignal,
  listServer, accessServer,
  putObject, getObject,
  putObjectLazy, getObjectLazy,
  ) where
-- }}}

-- import {{{
import Prelude (($), (.), (/), (-), (+), (>), (==),
                String, fromIntegral, id, Float, fst, Integral, Integer)
import Control.Applicative ((<$>))
import Control.Monad (Monad(..), void, when,)
import Control.Concurrent (forkIO, yield, ThreadId,)
import Control.Concurrent.MVar (MVar, takeMVar, newEmptyMVar, tryPutMVar,)
import Control.Exception (SomeException, Exception,
                          handle, bracket, catch, throwTo,)
import Data.Either (Either(..))
import Data.Serialize (Serialize(..), encode, decode, encodeLazy, decodeLazy, )
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int)
import Data.Bool (Bool(..))
import Data.IORef (IORef, newIORef, writeIORef, readIORef,)
import Data.List ((++), map, tail, break,)
import Data.Maybe (Maybe(..), maybe,)
import Data.Typeable (Typeable(..),)
import GHC.Generics (Generic)
import Network (HostName, PortID(..), Socket, connectTo,
                accept, listenOn, sClose,)
import Network.Socket(getNameInfo, SockAddr(..),)
--import System.Log.Logger (errorM, debugM, infoM, warningM, noticeM, )
--import System.CPUTime (getCPUTime,)
import System.IO (IO, hGetLine, hFlush, hPutStrLn, Handle, hClose, getLine,
                  hSetBuffering, BufferMode(..), putStrLn, hWaitForInput, stdin)
import qualified System.Posix.Signals as Sig
import Text.Read (read)
import Text.Show (Show(..))
import Text.Printf (printf)
import qualified Zookeeper as Zoo

-- }}}

-- data/type {{{

-- handler on a Handle (Socket or FD)
type IOHandler = AHandler ()
type AHandler a = Handle -> IO a

-- DServerInfo: on ZK. {{{
data DServerInfo = DServerInfo
  { dsiHostName   :: HostName,
    dsiPortNumber :: Integer,
    dsiServType   :: String
  } deriving (Show, Generic)
instance Serialize DServerInfo where

-- }}}
-- DServerData: on local process. {{{
data DServerData = DServerData
  { dsdThreadID   :: ThreadId,
    dsdPortNumber :: Integer,
    dsdZooKeeper  :: Zoo.ZHandle,
    dsdRunning    :: IORef Bool,
    dsdService    :: DService,
    dsdServerInfo :: DServerInfo
  }
instance Show DServerData where
  show (DServerData tid port zk _ _ info) =
    "DServerData:" ++ show (tid, port, zk, info)
-- }}}
-- DService: what it do {{{
data DService = DService
  { dsType    :: String,
    dsHandler :: IOHandler,
    dsCloseHook :: Maybe IOHandler
  }
-- }}}
-- ZKInfo: zookeeper info {{{
data ZKInfo = ZKInfo String deriving (Show)
-- }}}
-- CloseException: close server {{{
data CloseException = CloseException
  deriving (Typeable, Show)
instance Exception CloseException where
-- }}}
-- }}}

-- common {{{
-- commonInitial {{{
commonInitial :: IO ()
commonInitial = do
  void $ Sig.installHandler Sig.sigPIPE Sig.Ignore Nothing
  Zoo.setDebugLevel Zoo.LogError
-- }}}
-- getHostName {{{
getHostName :: IO HostName
getHostName = do
  mbhost <- fst <$> getNameInfo [] True False (SockAddrUnix "localhost")
  return $ maybe "localhost" id mbhost
-- }}}
-- portNumber {{{
portNumber :: Integral a => a -> PortID
portNumber a = PortNumber $ fromIntegral a
-- }}}
-- aHandler {{{
aHandler :: a -> SomeException -> IO a
aHandler a e = do
  putStrLn $ "aHandler caught Exception: " ++ show e
  return a
-- }}}
-- ioaHandler {{{
ioaHandler :: IO () -> a -> SomeException -> IO a
ioaHandler io a e = do
  putStrLn $ "ioaHandler caught Exception: " ++ show e
  io
  return a
-- }}}
-- }}}

-- server {{{
-- listenSS {{{
listenSS :: Integer -> IO (Maybe Socket)
listenSS port = (Just <$> listenOn pn) `catch` aHandler Nothing
  where pn = portNumber port
-- }}}
-- showDServerInfo {{{
showDServerInfo :: DServerInfo -> String
showDServerInfo (DServerInfo host port ty) =
  printf "%s:%s:%s" ty host (show port)
-- }}}
-- registerZK: connect && create -> Just handle {{{
registerZK :: ZKInfo -> DServerInfo -> IO (Maybe Zoo.ZHandle)
registerZK (ZKInfo hostport) dsi = do
  mbzh <- Zoo.initSafe hostport Nothing 100000
  case mbzh of
    Just zh -> do
      mbpath <- Zoo.createSafe zh zkName (Just $ encode dsi)
        Zoo.OpenAclUnsafe (Zoo.CreateMode True False)
      maybe (return Nothing) (\_ -> return mbzh) mbpath
    _ -> do
      return Nothing
  where zkName = "/d/" ++ showDServerInfo dsi
-- }}}
-- loopServer {{{
loopServer :: IORef Bool -> Socket -> DService -> IO ()
loopServer ref sock ds = do
  running <- readIORef ref
  when running $ do
    putStrLn "loopServer: waiting for client"
    oneServer sock ds `catch` ioaHandler (writeIORef ref False) ()
    putStrLn "loopServer: oneServer forked"
    loopServer ref sock ds
-- }}}
-- oneServer {{{
oneServer :: Socket -> DService -> IO ()
oneServer sock ds = do
  (h, _, _) <- accept sock
  hSetBuffering h $ BlockBuffering Nothing --LineBuffering
  void $ forkIO $ (dsHandler ds) h `catch` aHandler ()
-- }}}
-- forkServer {{{
forkServer :: ZKInfo -> DService -> Integer -> IO (Maybe DServerData)
forkServer zki ds port = do
  commonInitial
  hostname <- getHostName
  mbsock <- listenSS port
  case mbsock of
    Just sock -> do
      putStrLn "Listen OK"
      let dsi = DServerInfo hostname port (dsType ds)
      mbzk <- registerZK zki dsi
      case mbzk of
        Just zk -> do
          putStrLn "ZK OK"
          ref <- newIORef True
          tid <- forkIO $ loopServer ref sock ds
          return $ Just $ DServerData tid port zk ref ds dsi
        Nothing -> do
          sClose sock
          return Nothing
    Nothing -> do
      return Nothing
-- }}}
-- closeServer {{{
closeServer :: DServerData -> IO ()
closeServer dsd = do
  Zoo.close (dsdZooKeeper dsd)
  case dsCloseHook $ dsdService dsd of
    Just hook -> void $ accessServer (dsdServerInfo dsd) hook
    _ -> return ()
  throwTo (dsdThreadID dsd) CloseException
  yield
-- }}}
-- waitCloseCmd {{{
waitCloseCmd :: DServerData -> IO ()
waitCloseCmd dsd = do
  iP <- hWaitForInput stdin 1000000
  if iP
  then do
    line <- getLine
    case line of
      "x" -> Zoo.close (dsdZooKeeper dsd)
      _ -> waitCloseCmd dsd
  else waitCloseCmd dsd
-- }}}
-- waitCloseSignal {{{
waitCloseSignal :: DServerData -> IO ()
waitCloseSignal dsd = do
  (mvar :: MVar ()) <- newEmptyMVar
  installH Sig.sigTERM (sigHandler mvar)
  installH Sig.sigINT (sigHandler mvar)
  installH Sig.sigQUIT (sigHandler mvar)
  takeMVar mvar
  putStrLn "closed!"
  where
    sigHandler mvar = do
      void $ tryPutMVar mvar ()
      Zoo.close (dsdZooKeeper dsd)
    installH sig handler = void $ 
      Sig.installHandler sig (Sig.CatchOnce $ handler) Nothing
  
-- }}}
-- }}}

-- client {{{
-- listServer {{{
-- list online servers, lookup info. from ZK.
listServer :: ZKInfo -> IO (Maybe [DServerInfo])
listServer (ZKInfo hostport) = do
  mbzh <- Zoo.initSafe hostport Nothing 100000
  case mbzh of
    Just zh -> do
      -- putStrLn "client: init ok"
      mbChildList <- Zoo.getChildrenSafe zh "/d" Zoo.NoWatch
      case mbChildList of
        Just list -> return $ Just $ map parseDServerInfo list
        Nothing -> return Nothing
    Nothing -> return Nothing
-- }}}
-- parseDServerInfo {{{
parseDServerInfo :: String -> DServerInfo
parseDServerInfo str = DServerInfo host port ty
  where
    (ty,rest1) = break (== ':') str
    (host, rest2) = break (== ':') $ tail rest1
    port = fromIntegral $ (read $ tail rest2 :: Integer)
-- }}}
-- connectSocket {{{
connectSocket :: DServerInfo -> IO Handle
connectSocket dsi@(DServerInfo n p _) = do
  putStrLn $ show dsi
  h <- connectTo n $ portNumber p
  putStrLn "client -> Server ok"
  hSetBuffering h $ BlockBuffering Nothing --LineBuffering
  return h
-- }}}
-- accessServer {{{
accessServer :: DServerInfo -> AHandler a -> IO (Maybe a)
accessServer dsi handler = (Just <$> runner) `catch` exHandler
  where
    runner = bracket (connectSocket dsi) hClose handler
    exHandler (e :: SomeException) = do
      putStrLn $ "accessServer, error" ++ show e
      return Nothing
-- }}}
-- }}}

-- put/get BS {{{
-- put ByteString {{{
putBS :: Handle -> BS.ByteString -> IO ()
putBS hdl bs = do
  handle exHandler $ do
    hPutStrLn hdl $ (show $ BS.length bs) ++ " "
    BS.hPut hdl bs
    hFlush hdl
  where
    exHandler (_ :: SomeException) = return ()
-- }}}
-- get ByteString {{{
getBS :: Handle -> IO (BS.ByteString)
getBS hdl = do
  line <- hGetLine hdl
  bs <- BS.hGet hdl (read $ line :: Int)
  return bs
-- }}}
-- put Object on System.IO.Handle {{{
putObject :: (Serialize a) => Handle -> a -> IO ()
putObject h obj = putBS h (encode obj)
-- }}}
-- get Object on System.IO.Handle {{{
getObject :: (Serialize a) => Handle -> IO (Maybe a)
getObject h = do
  ea <- decode <$> getBS h
  case ea of
    Left _ -> return Nothing
    Right a -> return $ Just a
-- }}}
-- }}}

-- put/get BSL {{{
-- put ByteString {{{
putBSL :: Handle -> BSL.ByteString -> IO ()
putBSL hdl bs = do
  handle exHandler $ do
    hPutStrLn hdl $ (show $ BSL.length bs) ++ " "
    BSL.hPut hdl bs
    hFlush hdl
  where
    exHandler (_ :: SomeException) = return ()
-- }}}
-- get ByteString {{{
getBSL :: Handle -> IO (BSL.ByteString)
getBSL hdl = do
  line <- hGetLine hdl
  bs <- BSL.hGet hdl (read $ line :: Int)
  return bs
-- }}}
-- put Object on System.IO.Handle {{{
putObjectLazy :: (Serialize a) => Handle -> a -> IO ()
putObjectLazy h obj = putBSL h (encodeLazy obj)
-- }}}
-- get Object on System.IO.Handle {{{
getObjectLazy :: (Serialize a) => Handle -> IO (Maybe a)
getObjectLazy h = do
  ea <- decodeLazy <$> getBSL h
  case ea of
    Left _ -> return Nothing
    Right a -> return $ Just a
-- }}}
-- }}}

-- vim:fdm=marker

