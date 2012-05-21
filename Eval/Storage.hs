-- Copyright Wu Xingbo 2012

-- head {{{
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
-- }}}

-- module export {{{
module Eval.Storage where
-- }}}

-- import {{{
import qualified Data.Map as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Crypto.Hash.SHA1 as SHA1 -- hackage: cryptohash
import qualified Control.Concurrent.ReadWriteVar as RWVar
import qualified Control.Concurrent.ReadWriteLock as RWL
------
import Prelude (($), (.), (+), (>), (==), (++), (-), (/=),
                flip, Bool(..), compare, fromIntegral,
                IO, String, Show(..), Integer, id, toInteger,
                const, )
import Control.Applicative ((<$>), (<*>))
import Control.Monad (Monad(..), mapM_, when, unless, void)
import Control.Concurrent (MVar, readMVar, modifyMVar_, newMVar,
                           withMVar, forkIO, modifyMVar,)
import Control.Exception (catch,)
import Control.DeepSeq (deepseq)
import Data.Maybe (Maybe(..), maybe, isNothing)
import Text.Printf (printf)
--import Data.Word (Word64)
--import Data.Int (Int32, Int64)
import Data.Either (Either(..))
import System.IO (Handle, withFile, IOMode(..), withBinaryFile,
                  putStrLn, BufferMode(..), hSetBuffering,
                  openBinaryFile, hClose)
import Data.Tuple (swap,)
import Data.List (foldl', concatMap, map,)
--import System.IO.Error (userError, IOError, ioError)
import System.IO.Unsafe (unsafePerformIO)
import System.FilePath (FilePath, (</>),)
import System.Directory (removeFile, createDirectoryIfMissing)
import System.Posix.Time (epochTime)
import System.Posix.Files (getFileStatus, fileSize,)
import System.Posix.Process (getProcessID)
import System.Posix.Types (CPid(..))
import System.Time (ClockTime,)
import Foreign.C.Types (CTime(..))
import GHC.Generics (Generic)
import Data.Serialize (Serialize(..),)
import System.CPUTime.Rdtsc (rdtsc)
------
import Eval.DServ (AHandler, IOHandler, DService(..),
                   aHandler, ioaHandler, putObject, getObject)
-- }}}

-- data {{{
-- DSConfig {{{
data DSConfig = DSConfig
  { confMetaData :: FilePath,
    confRootPath :: FilePath }
    --confHost       :: String }
  deriving (Show, Generic)
instance Serialize DSConfig where
-- }}}
-- CheckSum {{{
type CheckSum = (Integer, String)
-- }}}
-- StorageAccessLog {{{
data StorageAccessLog = SALog [StorageAccess] deriving (Show, Generic)
data StorageAccess = SA
  { saClockTime :: ClockTime,
    saMessage   :: String }
  deriving (Show, Generic)
-- }}}
-- DSFile {{{
data DSFile
  = DSFile { dsCheckSum :: CheckSum,
             dsFilePath :: FilePath,
             dsRWLock   :: RWL.RWLock }
  deriving (Generic)
instance Serialize DSFile where
  put (DSFile sum path _) = put sum >> put path
  get = DSFile <$> get <*> get <*> (return $ unsafePerformIO RWL.new)
instance Show DSFile where
  show (DSFile sum path _) = "DSFile[" ++ show sum ++ path ++ "]"
-- }}}
-- DSData {{{
type DSData = BS.ByteString
-- }}}
-- DSNode {{{
data DSNode = DSNode
  { nodeFileM   :: MVar (Map.Map String DSFile),
    nodeCacheM  :: RWVar.RWVar (Map.Map String DSData),
    nodeConfig  :: DSConfig }
type DSNodeStatic = Map.Map String DSFile
-- }}}
-- DSReq {{{
-- req -> resp -> OP
data DSReq
  = DSRPutFile  String Integer
  | DSRPutCache String Integer
  | DSRGetFile  String
  | DSRGetCache String
  | DSRDelFile   String
  | DSRDelCache  String
  | DSRListFile
  | DSRListCache
  | DSRFreeze    String
  | DSRFreezeAll
  | DSRBackup
  deriving (Generic, Show)
instance Serialize DSReq where
-- }}}
-- DSResp {{{
data DSResp = DSRFail | DSROkay deriving (Generic, Show)
instance Serialize DSResp where
-- }}}
-- }}}

-- basic helper {{{

-- checkSum {{{
checkSumBS :: BS.ByteString -> CheckSum
checkSumBS bs = sum `deepseq` (fromIntegral (BS.length bs), sum)
  where sum = concatMap (printf "%02x") $ BS.unpack $ SHA1.hash bs

checkSumBSL :: BSL.ByteString -> CheckSum
checkSumBSL bs = sum `deepseq` (fromIntegral (BSL.length bs), sum)
  where sum = concatMap (printf "%02x") $ BS.unpack $ SHA1.hashlazy bs

checkSumFile :: FilePath -> IO CheckSum
checkSumFile path = do
  bs <- withBinaryFile path ReadMode BSL.hGetContents
  return $ checkSumBSL bs

checkSumDSPath :: DSNode -> FilePath -> IO CheckSum
checkSumDSPath node path = do
  h <- openLocalFile node path ReadMode
  checkSumBSL <$> BSL.hGetContents h
-- }}}

-- unique {{{
uniqueName :: IO FilePath
uniqueName = do
  (CTime clocktime) <- epochTime   -- Int64
  tick64 <- rdtsc  -- Word64
  (CPid pid) <- getProcessID -- Int32
  return $ printf "%016x-%016x-%08x" clocktime tick64 pid
-- }}}

-- fullDSFilePath {{{
fullDSFilePath :: DSNode -> DSFile -> FilePath
fullDSFilePath node dsfile =
  fullLocalFilePath node $ dsFilePath dsfile
-- }}}

-- fullLocalFilePath {{{
fullLocalFilePath :: DSNode -> FilePath -> FilePath
fullLocalFilePath node path =
  (confRootPath $ nodeConfig node) </> path
-- }}}

-- openBinBufFile {{{
openBinBufFile :: FilePath -> IOMode -> IO Handle
openBinBufFile path mode = do
  h <- openBinaryFile path mode
  hSetBuffering h $ BlockBuffering Nothing
  return h
-- }}}

-- openLocalFile {{{
openLocalFile :: DSNode -> FilePath -> IOMode -> IO Handle
openLocalFile node path mode = do
  let fullpath = fullLocalFilePath node path
  openBinBufFile fullpath mode
-- }}}

-- openDSFile {{{
openDSFile :: DSNode -> DSFile -> IOMode -> IO Handle
openDSFile node dsfile mode = do
  let fullpath = fullDSFilePath node dsfile
  openBinBufFile fullpath mode
-- }}}

-- getFileSize {{{
getFileSize :: FilePath -> IO Integer
getFileSize path = (toInteger . fileSize) <$> getFileStatus path
-- }}}

-- pipeSome {{{
pipeSome :: Integer -> Handle -> Handle -> IO ()
pipeSome rem from to = do
  BS.hGet from (fromIntegral seg) >>= BS.hPut to
  when (rem' /= 0) $ pipeSome rem' from to
  where
    lim = 0x100000
    seg = if rem > lim then lim else rem
    rem' = rem - seg
-- }}}

-- pipeAll {{{
pipeAll :: Handle -> Handle -> IO ()
pipeAll from to = do
  bs <- BS.hGet from lim
  BS.hPut to bs
  unless (BS.null bs) $ pipeAll from to
  where
    lim = 0x100000
-- }}}

-- respOK {{{
respOK :: Handle -> IO ()
respOK remoteH = putObject remoteH DSROkay
-- }}}

-- respFail {{{
respFail :: Handle -> IO ()
respFail remoteH = putObject remoteH DSRFail
-- }}}

-- }}}

-- DSFile {{{

-- pipeToDSFile {{{
pipeToDSFile :: DSNode -> Integer -> Handle -> IO DSFile
pipeToDSFile node count from = do
  path <- uniqueName
  --putStrLn $ "uniqueName: " ++ path
  to <- openLocalFile node path WriteMode
  pipeSome count from to
  hClose to
  chksum <- checkSumDSPath node path
  lock <- RWL.new
  return $ DSFile chksum path lock
-- }}}

-- bsToDSFile {{{
bsToDSFile :: DSNode -> BS.ByteString -> IO DSFile
bsToDSFile node bs = do
  path <- uniqueName
  to <- openLocalFile node path WriteMode
  BS.hPut to bs
  lock <- RWL.new
  let chksum = checkSumBS bs
  return $ DSFile chksum path lock
-- }}}

-- pipeToRemote {{{
pipeToRemote :: DSNode -> DSFile -> Handle -> IO ()
pipeToRemote node dsfile to = do
  from <- openDSFile node dsfile ReadMode
  pipeAll from to
-- }}}

-- loadDSFileToBS {{{
loadDSFileToBS :: DSNode -> DSFile -> IO BS.ByteString
loadDSFileToBS node dsfile = do
  from <- openDSFile node dsfile ReadMode
  BS.hGetContents from
-- }}}

-- localDSFileSize {{{
localDSFileSize :: DSNode -> DSFile -> IO Integer
localDSFileSize node dsfile = getFileSize $ fullDSFilePath node dsfile
-- }}}

-- deleteDSFile {{{
deleteDSFile :: DSNode -> DSFile -> IO ()
deleteDSFile node dsfile = do
  let filename = fullDSFilePath node dsfile
  removeFile filename
-- }}}

-- }}}

-- DSNode {{{

-- remote
-- nodeRemoteReadFile {{{
-- uses read lock on file
nodeRemoteReadFile :: DSNode -> String -> Handle -> IO ()
nodeRemoteReadFile node name remoteH = do
  putStrLn $ "remote read file: " ++ name
  mbdsfile <- nodeLookupFile node name
  case mbdsfile of
    Just dsfile -> RWL.withRead (dsRWLock dsfile) $ do
      respOK remoteH
      size <- localDSFileSize node dsfile
      putObject remoteH size
      pipeToRemote node dsfile remoteH
      respOK remoteH
    _ -> do
      respFail remoteH
      return ()
-- }}}

-- nodeRemoteReadCache {{{
nodeRemoteReadCache :: DSNode -> String -> Handle -> IO ()
nodeRemoteReadCache node name remoteH = do
  mbbs <- nodeLookupCache node name
  case mbbs of
    Just bs -> do
      respOK remoteH
      putObject remoteH $ BS.length bs
      BS.hPut remoteH bs
      respOK remoteH
    _ -> do
      nodeRemoteReadFile node name remoteH
      void $ forkIO $ nodeLocalCacheFile node name
-- }}}

-- nodeRemoteWriteFile {{{
nodeRemoteWriteFile :: DSNode -> String -> Integer -> Handle -> IO ()
nodeRemoteWriteFile node name size remoteH = do
  respOK remoteH
  nodeLocalDeleteCache node name
  nodeLocalDeleteFile node name
  dsfile <- pipeToDSFile node size remoteH
  modifyMVar_ (nodeFileM node) $ return . Map.insert name dsfile
  respOK remoteH
-- }}}

-- nodeRemoteWriteCache {{{
nodeRemoteWriteCache :: DSNode -> String -> Integer -> Handle -> IO ()
nodeRemoteWriteCache node name size remoteH = do
  respOK remoteH
  bs <- BS.hGet remoteH (fromIntegral size)
  RWVar.modify_ (nodeCacheM node) $ return . Map.insert name bs
  respOK remoteH
  void $ forkIO $ nodeLocalDumpFile node name
-- }}}

-- nodeRemoteDeleteFile {{{
nodeRemoteDeleteFile :: DSNode -> String -> Handle -> IO ()
nodeRemoteDeleteFile node name remoteH = do
  nodeLocalDeleteFile node name
  respOK remoteH
-- }}}

-- nodeRemoteDeleteCache {{{
nodeRemoteDeleteCache :: DSNode -> String -> Handle -> IO ()
nodeRemoteDeleteCache node name remoteH = do
  nodeLocalDeleteCache node name
  respOK remoteH
-- }}}

-- nodeRemoteListFile {{{
nodeRemoteListFile :: DSNode -> Handle -> IO ()
nodeRemoteListFile node remoteH = do
  respOK remoteH
  filemap <- readMVar (nodeFileM node)
  putObject remoteH $ map show $ Map.toList filemap
  respOK remoteH
-- }}}

-- nodeRemoteListCache {{{
nodeRemoteListCache :: DSNode -> Handle -> IO ()
nodeRemoteListCache node remoteH = do
  respOK remoteH
  cachemap <- RWVar.with (nodeCacheM node) $ return
  putObject remoteH $ map show $ Map.toList $ Map.map (BS.length) cachemap
  respOK remoteH
-- }}}

-- local basic
-- nodeLookupFile {{{
nodeLookupFile :: DSNode -> String -> IO (Maybe DSFile)
nodeLookupFile node name = do
  withMVar (nodeFileM node) $ return . Map.lookup name
-- }}}

-- nodeLookupCache {{{
nodeLookupCache :: DSNode -> String -> IO (Maybe DSData)
nodeLookupCache node name = do
  RWVar.with (nodeCacheM node) $ return . Map.lookup name
-- }}}

-- nodeMemoryUsage {{{
nodeMemoryUsage :: DSNode -> IO Integer
nodeMemoryUsage node = do
  cacheM <- RWVar.with (nodeCacheM node) return
  return $ foldl' plus 0 $ Map.elems cacheM
  where
    plus orig bs = orig + (fromIntegral $ BS.length bs)
-- }}}

-- local advanced
-- nodeLocalDumpFile {{{
nodeLocalDumpFile :: DSNode -> String -> IO ()
nodeLocalDumpFile node name = do
  mbdsfile <- nodeLookupFile node name
  when (isNothing mbdsfile) $ do
    mbbs <- nodeLookupCache node name
    case mbbs of
      Just bs -> do
        dsfile <- bsToDSFile node bs
        modifyMVar_ (nodeFileM node) $ return . Map.insert name dsfile
      _ -> return ()
-- }}}

-- nodeLocalFreezeData {{{
nodeLocalFreezeData :: DSNode -> String -> IO ()
nodeLocalFreezeData node name = do
  nodeLocalDumpFile node name
  nodeLocalDeleteCache node name
-- }}}

-- nodeLocalCacheFile {{{
nodeLocalCacheFile :: DSNode -> String -> IO ()
nodeLocalCacheFile node name = do
  mbbs <- nodeLookupCache node name
  when (isNothing mbbs) $ do
    mbdsfile <- nodeLookupFile node name
    case mbdsfile of
      Just dsfile -> do
        bs <- RWL.withRead (dsRWLock dsfile) $ loadDSFileToBS node dsfile
        RWVar.modify_ (nodeCacheM node) $ return . Map.insert name bs
      _ -> return ()
-- }}}

-- nodeLocalBackupMeta {{{
-- just backup the fileM (Map.Map String DSFile)
nodeLocalBackupMeta :: DSNode -> IO ()
nodeLocalBackupMeta node = do
  filemap <- readMVar (nodeFileM node)
  withFile (confMetaData (nodeConfig node)) WriteMode (flip putObject filemap)
-- }}}

-- nodeLocalFreezeAll {{{
nodeLocalFreezeAll :: DSNode -> IO ()
nodeLocalFreezeAll node = do
  list <- RWVar.with (nodeCacheM node) $ return . Map.keys
  mapM_ (nodeLocalFreezeData node) list
  nodeLocalBackupMeta node
-- }}}

-- nodeLocalDeleteFile {{{
nodeLocalDeleteFile :: DSNode -> String -> IO ()
nodeLocalDeleteFile node name = do
  mbdsfile <- modifyMVar (nodeFileM node) $
    return . swap . Map.updateLookupWithKey (const . const $ Nothing) name
  case mbdsfile of
    Just dsfile -> do
      RWL.withWrite (dsRWLock dsfile) $ do
      deleteDSFile node dsfile
    _ -> return ()
-- }}}

-- nodeLocalDeleteCache {{{
nodeLocalDeleteCache :: DSNode -> String -> IO ()
nodeLocalDeleteCache node name = do
  RWVar.modify_ (nodeCacheM node) $ return . Map.delete name
-- }}}

-- }}}

-- Storage Server {{{

-- make service {{{
makeDSService :: FilePath -> IO DService
makeDSService path = do
  conf <- makeConf path
  node <- loadMetaNode conf
  return $ DService "DStorage" (storageHandler node) (Just closer)
-- }}}

-- config file {{{
makeConf :: FilePath -> IO DSConfig
makeConf path = do
  let rootdir = path ++ "/data"
  createDirectoryIfMissing True rootdir
  return $ DSConfig (path ++ "/meta") rootdir
-- }}}

-- loadMetaNode {{{
loadMetaNode :: DSConfig -> IO DSNode
loadMetaNode conf = do
  mbmeta <- (withFile (confMetaData conf) ReadMode getObject) `catch` aHandler Nothing
  fileM <- case mbmeta of
    Just meta -> newMVar meta
    _ -> newMVar Map.empty
  cacheM <- RWVar.new Map.empty
  return $ DSNode fileM cacheM conf
-- }}}

-- main handler {{{
storageHandler :: DSNode -> IOHandler
storageHandler node remoteH = do
  mbReq <- getObject remoteH
  putStrLn $ "recv request:" ++ show mbReq
  case mbReq of
    Just req -> handleReq node remoteH req
    _ -> putObject remoteH DSRFail
-- }}}

-- request handler {{{
handleReq :: DSNode -> Handle -> DSReq -> IO ()
handleReq node remoteH req = do
  case req of
    -- Get
    (DSRGetFile name) -> respDo $ nodeRemoteReadFile node name remoteH
    (DSRGetCache name) -> respDo $ nodeRemoteReadCache node name remoteH
    -- Put
    (DSRPutFile name len) -> do
      putStrLn $ "processing PutFile"
      if len > 0x10000000
      then respFail remoteH
      else respDo $ nodeRemoteWriteFile node name len remoteH
    (DSRPutCache name len) -> if len > 0x10000000
      then respFail remoteH
      else respDo $ nodeRemoteWriteCache node name len remoteH
    -- List
    (DSRListFile) -> respDo $ nodeRemoteListFile node remoteH
    (DSRListCache) -> respDo $ nodeRemoteListCache node remoteH
    -- Del
    (DSRDelFile name) -> respDo $ nodeRemoteDeleteFile node name remoteH
    (DSRDelCache name) -> respDo $ nodeRemoteDeleteCache node name remoteH
    -- Freeze
    (DSRFreeze name) -> respOKDo $ nodeLocalFreezeData node name
    (DSRFreezeAll) -> respOKDo $ nodeLocalFreezeAll node
    -- 
    (DSRBackup) -> respOKDo $ nodeLocalBackupMeta node
  where
    respDo op = op `catch` (ioaHandler (respFail remoteH) ())
    respOKDo op = respDo (op >> respOK remoteH)
-- }}}

-- closer {{{
closer :: IOHandler
closer h = do
  putObject h DSRFreezeAll
  void $ (getObject h :: IO (Maybe DSResp))
  return ()
-- }}}

-- }}}

-- Storage Client {{{

-- clientPut {{{
clientPut :: Bool -> String -> FilePath -> AHandler Bool
clientPut cache name filepath remoteH = do
  size <- getFileSize filepath
  putObject remoteH $ (if cache then DSRPutCache else DSRPutFile) name size
  (mbResp1 :: Maybe DSResp) <- getObject remoteH
  case mbResp1 of
    Just DSROkay -> do
      openBinBufFile filepath ReadMode >>= flip pipeAll remoteH
      (mbResp2 :: Maybe DSResp) <- getObject remoteH
      case mbResp2 of
        Just DSROkay -> return True
        Just DSRFail -> putStrLn "resp2 failed" >> return False
        _ -> putStrLn "recv resp2 failed" >> return False
    Just DSRFail -> putStrLn "resp1 failed" >> return False
    _ -> putStrLn "recv resp1 failed" >> return False
-- }}}

-- clientGet {{{
clientGet :: Bool -> String -> FilePath -> AHandler Bool
clientGet cache name localpath remoteH = do
  putObject remoteH $ (if cache then DSRGetCache else DSRGetFile) name
  (mbResp1 :: Maybe DSResp) <- getObject remoteH
  case mbResp1 of
    Just DSROkay -> do
      (mbSize :: Maybe Integer) <- getObject remoteH
      case mbSize of
        Just size -> do
          localH <- openBinBufFile localpath WriteMode
          pipeSome size remoteH localH
          (mbResp2 :: Maybe DSResp) <- getObject remoteH
          case mbResp2 of
            Just DSROkay -> return True
            Just DSRFail -> putStrLn "resp2 failed" >> return False
            _ -> putStrLn "recv resp2 failed" >> return False
        _ -> putStrLn "get size failed" >> return False
    _ -> putStrLn "get resp1 failed" >> return False
-- }}}

-- clientNoRecv {{{
clientNoRecv :: DSReq -> AHandler Bool
clientNoRecv req remoteH = do
  putObject remoteH req
  (mbResp1 :: Maybe DSResp) <- getObject remoteH
  case mbResp1 of
    Just DSROkay -> return True
    Just DSRFail -> putStrLn "resp failed" >> return False
    _ -> putStrLn "recv resp failed" >> return False
-- }}}

-- client* with name {{{
clientDelFile :: String -> AHandler Bool
clientDelFile name = clientNoRecv $ DSRDelFile name

clientDelCache :: String -> AHandler Bool
clientDelCache name = clientNoRecv $ DSRDelCache name

clientFreeze :: String -> AHandler Bool
clientFreeze name = clientNoRecv $ DSRFreeze name
-- }}}

-- client* {{{
clientBackup :: AHandler Bool
clientBackup = clientNoRecv DSRBackup

clientFreezeAll :: AHandler Bool
clientFreezeAll = clientNoRecv DSRFreezeAll
-- }}}

-- clientList {{{
clientList :: Bool -> AHandler [String]
clientList cache remoteH = do
  putObject remoteH $ (if cache then DSRListCache else DSRListFile)
  (mbResp1 :: Maybe DSResp) <- getObject remoteH
  case mbResp1 of
    Just DSROkay -> do
      lst <- getObject remoteH
      (mbResp2 :: Maybe DSResp) <- getObject remoteH
      case mbResp2 of
        Just DSROkay -> return $ maybe [] id lst
        Just DSRFail -> (putStrLn $ "resp2 fail" ++ show lst) >> return []
        _ -> (putStrLn $ "recv resp2 fail" ++ show lst) >> return []
    Just DSRFail -> putStrLn "resp1 failed" >> return []
    _ -> putStrLn "recv resp1 failed" >> return []
-- }}}

-- clientH {{{
clientH :: [String] -> AHandler (Either Bool [String])
clientH ("put":name:filename:_) = (Left <$>) . clientPut False name filename
clientH ("putc":name:filename:_) = (Left <$>) . clientPut True name filename
clientH ("get":name:filename:_) = (Left <$>) . clientGet False name filename
clientH ("getc":name:filename:_) = (Left <$>) . clientGet True name filename
clientH ("del":name:_) = (Left <$>) . clientDelFile name
clientH ("delc":name:_) = (Left <$>) . clientDelCache name
clientH ("ls":_) = (Right <$>) . clientList False
clientH ("lsc":_) = (Right <$>) . clientList True
clientH ("freeze":name:_) = (Left <$>) . clientFreeze name
clientH ("freeze":_) = (Left <$>) . clientFreezeAll
clientH ("backup":_) = (Left <$>) . clientBackup
clientH cmd = const $ (putStrLn $ "??: " ++ show cmd) >> (return $ Left False)
-- }}}

-- }}}

-- vim: fdm=marker
