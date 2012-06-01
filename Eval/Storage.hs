-- Copyright Wu Xingbo 2012

-- head {{{
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE RecordWildCards #-}
-- }}}

-- module export {{{
module Eval.Storage (
  DStorConfig, DSReq(..), CheckSum,
  DSNode(..), DSDirInfo, DSFile(..), DSData,
  checkSumBS, checkSumBSL, checkSumFile, checkSumDSPath,
  uniqueName, openBinBufFile, getFileSize,
  pipeSome, pipeAll,
  makeDSService, storageServiceType,
  clientPutFile, clientPutBS, clientPutH,
  clientGetFile, clientGetH,
  clientList, clientDel,
  clientFreeze, clientFreezeAll, clientCacheSize,
  clientGetSum, clientVerify, clientBackup,
  listOnlineStorage,
  runStorSimpleServer, runStorClientREPL,
  ) where
-- }}}

-- import {{{
import qualified Data.Map as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Crypto.Hash.SHA1 as SHA1 -- hackage: cryptohash
import qualified Control.Concurrent.ReadWriteVar as RWVar
import qualified Control.Concurrent.ReadWriteLock as RWL
------
import Prelude (($), (.), (+), (>), (==), (++), (-), (/=), (>=), (||),
                flip, Bool(..), compare, fromIntegral, fst,
                IO, String, Show(..), Integer, id, toInteger, Int,
                const, read)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (Monad(..), mapM_, when, unless, void)
import Control.Concurrent (MVar, readMVar, modifyMVar_, newMVar,
                           withMVar, forkIO, modifyMVar,)
import Control.Exception (catch,)
import Control.DeepSeq (deepseq)
import Data.Maybe (Maybe(..), maybe, isNothing)
import Text.Printf (printf)
import Data.Either (Either(..))
import System.IO (Handle, withFile, IOMode(..),
                  putStrLn, BufferMode(..), hSetBuffering,
                  openBinaryFile, hClose, hFlush, stdout, getLine)
import Data.Tuple (swap,)
import Data.List (foldl', concatMap, map, filter, all, elem, (!!),
                  zip, words, length, head,)
import System.IO.Unsafe (unsafePerformIO)
import System.FilePath (FilePath, (</>),)
import System.Directory (removeFile, createDirectoryIfMissing)
import System.Posix.Time (epochTime)
import System.Posix.Files (getFileStatus, fileSize,)
import System.Posix.Process (getProcessID)
import System.Posix.Types (CPid(..))
import Foreign.C.Types (CTime(..))
import GHC.Generics (Generic)
import Data.Serialize (Serialize(..),)
import System.CPUTime.Rdtsc (rdtsc)
------
import Eval.DServ (AHandler, IOHandler, DService(..),
                   ZKInfo(..), DServerInfo(..),
                   commonInitial, forkServer, waitCloseSignal,
                   aHandler, ioaHandler, putObject, getObject,
                   listServer, accessServer,
                   DResp(..), respOK, respFail,
                   clientTwoStage, clientRecvA, clientNoRecv,)
-- }}}

-- data {{{
-- DStorConfig {{{
data DStorConfig = DStorConfig
  { confMetaData :: FilePath,
    confRootPath :: FilePath }
    --confHost       :: String }
  deriving (Show, Generic)
instance Serialize DStorConfig where
-- }}}
-- CheckSum {{{
type CheckSum = (Integer, String)
-- }}}
-- StorageAccessLog {{{
--data StorageAccessLog = SALog [StorageAccess] deriving (Show, Generic)
--data StorageAccess = SA
--  { saClockTime :: ClockTime,
--    saMessage   :: String }
--  deriving (Show, Generic)
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
    nodeConfig  :: DStorConfig }
--type DSNodeStatic = Map.Map String DSFile
-- }}}
-- DSDirInfo {{{
type DSDirInfo = Map.Map String CheckSum
-- }}}
-- DSReq {{{
-- req -> resp -> OP
data DSReq
  = DSRPutFile    String CheckSum
  | DSRPutCache   String CheckSum
  | DSRGetFile    String
  | DSRGetCache   String
  | DSRDelFile    String
  | DSRDelCache   String
  | DSRListFile
  | DSRListCache
  | DSRFreeze     String
  | DSRFreezeAll
  | DSRBackup
  | DSRCacheSize
  | DSRVerify     String (Maybe CheckSum) -- verify by checksum, delete on fail
  | DSRGetSum     String
  | DSRDupFile    String DServerInfo
  | DSRDupCache   String DServerInfo
  deriving (Generic, Show)
instance Serialize DSReq where
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
  h <- openBinBufFile path ReadMode
  checkSumBSL <$> BSL.hGetContents h

checkSumDSPath :: DSNode -> FilePath -> IO CheckSum
checkSumDSPath node path = do
  h <- openLocalFile node path ReadMode
  checkSumBS <$> BS.hGetContents h

verifyDSFile :: DSNode -> DSFile -> Maybe CheckSum -> IO Bool
verifyDSFile node dsfile mbsum = do
  if maybe True (dssum ==) mbsum
  then do
    ((dssum ==) <$> checkSumDSPath node dspath) `catch` aHandler False
  else return False
  where
    dssum = dsCheckSum dsfile
    dspath = dsFilePath dsfile
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

-- pipeSeg {{{
pipeSeg :: Int -> Handle -> Handle -> IO ()
pipeSeg size from to = BS.hGet from size >>= BS.hPut to
-- }}}

-- pipeSome {{{
pipeSome :: Integer -> Handle -> Handle -> IO ()
pipeSome rem from to = do
  pipeSeg (fromIntegral seg) from to
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

-- }}}

-- DSFile {{{

-- pipeToDSFile {{{
pipeToDSFile :: DSNode -> Integer -> Handle -> IO (Maybe DSFile)
pipeToDSFile node count from = do
  path <- uniqueName
  --putStrLn $ "uniqueName: " ++ path
  to <- openLocalFile node path WriteMode
  pipeSome count from to
  hClose to
  chksum <- checkSumDSPath node path
  dsfile <- DSFile chksum path <$> RWL.new
  if fst chksum == count
  then return $ Just dsfile
  else do
    deleteDSFile node dsfile
    return Nothing
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
      putObject remoteH $ toInteger $ BS.length bs
      BS.hPut remoteH bs
      respOK remoteH
    _ -> do
      nodeRemoteReadFile node name remoteH
      void $ forkIO $ nodeLocalCacheFile node name
-- }}}

-- nodeRemoteWriteFile {{{
nodeRemoteWriteFile :: DSNode -> String -> CheckSum -> Handle -> IO ()
nodeRemoteWriteFile node name sum remoteH = do
  respOK remoteH
  nodeLocalDeleteCache node name
  nodeLocalDeleteFile node name
  putStrLn "start pipe to DSFile"
  mbdsfile <- pipeToDSFile node (fst sum) remoteH
  putStrLn "finish pipe to DSFile"
  case mbdsfile of
    Just dsfile -> do
      modifyMVar_ (nodeFileM node) $ return . Map.insert name dsfile
      respOK remoteH
    _ -> respFail remoteH
-- }}}

-- nodeRemoteWriteCache {{{
nodeRemoteWriteCache :: DSNode -> String -> CheckSum -> Handle -> IO ()
nodeRemoteWriteCache node name sum remoteH = do
  respOK remoteH
  putStrLn "write cache started"
  bs <- BS.hGet remoteH (fromIntegral $ fst sum)
  putStrLn "write cache finished"
  RWVar.modify_ (nodeCacheM node) $ return . Map.insert name bs
  respOK remoteH
  --void $ forkIO $ nodeLocalDumpFile node name
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
  putObject remoteH (Map.map (dsCheckSum) filemap :: DSDirInfo)
  respOK remoteH
-- }}}

-- nodeRemoteListCache {{{
nodeRemoteListCache :: DSNode -> Handle -> IO ()
nodeRemoteListCache node remoteH = do
  respOK remoteH
  cachemap <- RWVar.with (nodeCacheM node) $ return
  putObject remoteH (Map.map checkSumBS cachemap :: DSDirInfo)
  respOK remoteH
-- }}}

-- nodeRemoteMemoryUsage {{{
nodeRemoteMemoryUsage :: DSNode -> Handle -> IO ()
nodeRemoteMemoryUsage node remoteH = do
  respOK remoteH
  size <- nodeLocalMemoryUsage node
  putObject remoteH size
  respOK remoteH
-- }}}

-- nodeRemoteVerify {{{
nodeRemoteVerify :: DSNode -> String -> (Maybe CheckSum) -> Handle -> IO ()
nodeRemoteVerify node name mbsum remoteH = do
  respOK remoteH
  ok <- nodeLocalVerify node name mbsum
  putObject remoteH ok
  respOK remoteH
-- }}}

-- nodeRemoteGetSum {{{
nodeRemoteGetSum :: DSNode -> String -> Handle -> IO ()
nodeRemoteGetSum node name remoteH = do
  mbdsfile <- nodeLookupFile node name
  case mbdsfile of
    Just dsfile -> do
      respOK remoteH
      putObject remoteH (dsCheckSum dsfile)
      respOK remoteH
    _ -> do
      mbdata <- nodeLookupCache node name
      case mbdata of
        Just bs -> do
          respOK remoteH
          putObject remoteH $ checkSumBS bs
          respOK remoteH
        _ -> respFail remoteH
-- }}}

-- nodeRemoteDupFile {{{
nodeRemoteDup :: Bool -> DSNode -> String -> DServerInfo -> Handle -> IO ()
nodeRemoteDup cache node name target remoteH = do
  respOK remoteH
  mbbs <- nodeLookupCache node name
  case mbbs of
    Just bs -> do
      mbok <- accessServer target (clientPutBS cache name bs)
      case mbok of
        Just True -> respOK remoteH
        _ -> respFail remoteH
    _ -> do
      mbdsfile <- nodeLookupFile node name
      case mbdsfile of
        Just dsfile -> do
          localH <- openDSFile node dsfile ReadMode
          let sum = dsCheckSum dsfile
          mbok <- accessServer target (clientPutH cache name localH sum)
          case mbok of
            Just True -> respOK remoteH
            _ -> respFail remoteH
        _ -> respFail remoteH
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

-- nodeLocalMemoryUsage {{{
nodeLocalMemoryUsage :: DSNode -> IO Integer
nodeLocalMemoryUsage node = do
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
  withFile (confMetaData $ nodeConfig node) WriteMode (flip putObject filemap)
-- }}}

-- nodeLocalFreezeAll {{{
nodeLocalFreezeAll :: DSNode -> IO ()
nodeLocalFreezeAll node = do
  list <- RWVar.with (nodeCacheM node) $ return . Map.keys
  mapM_ (nodeLocalFreezeData node) list
  --nodeLocalBackupMeta node
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

-- nodeLocalVerify {{{
nodeLocalVerifyFile :: DSNode -> String -> Maybe CheckSum -> IO (Maybe Bool)
nodeLocalVerifyFile node name mbsum = do
  mbdsfile <- nodeLookupFile node name
  mbdsfileOK <- case mbdsfile of
    Just dsfile -> Just <$> verifyDSFile node dsfile mbsum
    _ -> return Nothing
  when (mbdsfileOK == Just False) $ do
    nodeLocalDeleteFile node name
  return mbdsfileOK

nodeLocalVerifyCache :: DSNode -> String -> CheckSum -> IO (Maybe Bool)
nodeLocalVerifyCache node name sum = do
  mbcachesum <- (checkSumBS <$>) <$> nodeLookupCache node name
  sumOK <- case mbcachesum of
    Just cachesum -> return $ Just $ sum == cachesum
    _ -> return Nothing
  when (sumOK == Just False) $ do
    nodeLocalDeleteCache node name
  return sumOK

nodeLocalVerify :: DSNode -> String -> Maybe CheckSum -> IO Bool
nodeLocalVerify node name mbsum@(Just sum) = do
  mbdsfileOK <- nodeLocalVerifyFile node name mbsum
  mbcacheOK <- nodeLocalVerifyCache node name sum
  putStrLn $ "localverify:" ++ show (mbdsfileOK, mbcacheOK)
  case (mbdsfileOK, mbcacheOK) of
    (Just True, _) -> return True
    (_, Just True) -> return True
    _ -> return False

nodeLocalVerify node name Nothing = do
  mbdsfileOK <- nodeLocalVerifyFile node name Nothing
  mbsum <- (dsCheckSum <$>) <$> nodeLookupFile node name
  mbcacheOK <- case mbsum of
    Just sum -> nodeLocalVerifyCache node name sum
    _ -> maybe Nothing (const $ Just True) <$> nodeLookupCache node name
  putStrLn $ "localverify:" ++ show (mbdsfileOK, mbcacheOK)
  case (mbdsfileOK, mbcacheOK) of
    (Just True, _) -> return True
    (_, Just True) -> return True
    _ -> return False
-- }}}

-- }}}

-- Storage Server {{{

storageServiceType :: String
storageServiceType = "DStorage"

-- make service {{{
makeDSService :: FilePath -> IO DService
makeDSService path = do
  conf <- makeConf path
  node <- loadNodeMeta conf
  return $ DService storageServiceType (storageHandler node) (Just closeStorage)
-- }}}

-- config file {{{
makeConf :: FilePath -> IO DStorConfig
makeConf path = do
  let rootdir = path ++ "/data"
  createDirectoryIfMissing True rootdir
  return $ DStorConfig (path ++ "/meta") rootdir
-- }}}

-- loadMetaNode {{{
loadNodeMeta :: DStorConfig -> IO DSNode
loadNodeMeta conf = do
  mbmeta <- loader `catch` aHandler Nothing
  fileM <- case mbmeta of
    Just meta -> newMVar meta
    _ -> newMVar Map.empty
  cacheM <- RWVar.new Map.empty
  return $ DSNode fileM cacheM conf
  where loader = withFile (confMetaData conf) ReadMode getObject
-- }}}

-- main handler {{{
storageHandler :: DSNode -> IOHandler
storageHandler node remoteH = do
  mbReq <- getObject remoteH
  putStrLn $ "recv request:" ++ show mbReq
  case mbReq of
    Just req -> handleReq node remoteH req
    _ -> respFail remoteH
-- }}}

-- request handler {{{
handleReq :: DSNode -> Handle -> DSReq -> IO ()
handleReq node remoteH req = do
  case req of
    -- Get
    (DSRGetFile name) ->
      respDo $ nodeRemoteReadFile node name remoteH
    (DSRGetCache name) ->
      respDo $ nodeRemoteReadCache node name remoteH
    -- Put
    (DSRPutFile name sum) -> do
      putStrLn $ "processing PutFile"
      if fst sum > 0x10000000
      then respFail remoteH
      else respDo $ nodeRemoteWriteFile node name sum remoteH
    (DSRPutCache name sum) -> if fst sum > 0x10000000
      then respFail remoteH
      else respDo $ nodeRemoteWriteCache node name sum remoteH
    -- List
    (DSRListFile) ->
      respDo $ nodeRemoteListFile node remoteH
    (DSRListCache) ->
      respDo $ nodeRemoteListCache node remoteH
    -- Del
    (DSRDelFile name) ->
      respDo $ nodeRemoteDeleteFile node name remoteH
    (DSRDelCache name) ->
      respDo $ nodeRemoteDeleteCache node name remoteH
    -- Freeze
    (DSRFreeze name) ->
      respOKDo $ nodeLocalFreezeData node name
    (DSRFreezeAll) ->
      respOKDo $ nodeLocalFreezeAll node
    -- Dump
    (DSRBackup) ->
      respOKDo $ nodeLocalBackupMeta node
    -- Cache size
    (DSRCacheSize) ->
      respDo $ nodeRemoteMemoryUsage node remoteH
    (DSRVerify name mbsum) ->
      respDo $ nodeRemoteVerify node name mbsum remoteH
    (DSRGetSum name) ->
      respDo $ nodeRemoteGetSum node name remoteH
    (DSRDupFile name target) ->
      respDo $ nodeRemoteDup False node name target remoteH
    (DSRDupCache name target) ->
      respDo $ nodeRemoteDup True node name target remoteH
  where
    respDo op = op `catch` (ioaHandler (respFail remoteH) ())
    respOKDo op = respDo (op >> respOK remoteH)
-- }}}

-- closeStorage {{{
closeStorage :: IOHandler
closeStorage h = do
  putObject h DSRFreezeAll
  void $ (getObject h :: IO (Maybe DResp))
  return ()
-- }}}

-- }}}

-- Storage Client {{{

-- clientPutFile {{{
clientPutFile :: Bool -> String -> FilePath -> AHandler Bool
clientPutFile cache name filepath remoteH = do
  putStrLn $ "start get chksum:"
  chksum <- checkSumFile filepath
  putStrLn $ "sum:" ++ show chksum
  clientTwoStage (req chksum) putData remoteH
  where
    req chksum = (if cache then DSRPutCache else DSRPutFile) name chksum
    putData rH = do
      putStrLn "start send file content"
      openBinBufFile filepath ReadMode >>= flip pipeAll rH
      putStrLn "finish send file content"
      return True
-- }}}

-- clientPutBS {{{
clientPutBS :: Bool -> String -> BS.ByteString -> AHandler Bool
clientPutBS cache name bs remoteH = do
  clientTwoStage req putData remoteH
  where
    sum = checkSumBS bs
    req = (if cache then DSRPutCache else DSRPutFile) name sum
    putData rH = BS.hPut rH bs >> return True
-- }}}

-- clientPutH {{{
clientPutH :: Bool -> String -> Handle -> CheckSum -> AHandler Bool
clientPutH cache name from sum remoteH = do
  clientTwoStage req pipeData remoteH
  where
    req = (if cache then DSRPutCache else DSRPutFile) name sum
    pipeData rH = pipeSome (fst sum) from rH >> return True
-- }}}

-- clientGetFile {{{
clientGetFile :: Bool -> String -> FilePath -> AHandler Bool
clientGetFile cache name localpath remoteH = do
  clientTwoStage req getFile remoteH
  where
    req = (if cache then DSRGetCache else DSRGetFile) name
    getFile rH = do
      (mbSize :: Maybe Integer) <- getObject rH
      case mbSize of
        Just size -> do
          localH <- openBinBufFile localpath WriteMode
          pipeSome size rH localH
          return True
        _ -> return False
-- }}}

-- clientGetH {{{
clientGetH :: Bool -> String -> Handle -> AHandler Bool
clientGetH cache name localH remoteH = do
  clientTwoStage req getH remoteH
  where
    req = (if cache then DSRGetCache else DSRGetFile) name
    getH rH = do
      (mbSize :: Maybe Integer) <- getObject rH
      case mbSize of
        Just size -> do
          pipeSome size rH localH
          return True
        _ -> return False
-- }}}

-- client* with name {{{
clientDel :: Bool -> String -> AHandler Bool
clientDel cache name = clientNoRecv $
  if cache then DSRDelCache name else DSRDelFile name

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
clientList :: Bool -> AHandler DSDirInfo
clientList cache remoteH = do
  mbmap <- clientRecvA (if cache then DSRListCache else DSRListFile) remoteH
  return $ maybe Map.empty id mbmap
-- }}}

-- clientCacheSize {{{
clientCacheSize :: AHandler Integer
clientCacheSize remoteH = do
  maybe 0 id <$> clientRecvA DSRCacheSize remoteH
-- }}}

-- clientVerify {{{
clientVerify :: String -> Maybe CheckSum -> AHandler Bool
clientVerify name mbchecksum remoteH = do
  maybe False id <$> clientRecvA (DSRVerify name mbchecksum) remoteH
-- }}}

-- clientGetSum {{{
clientGetSum :: String -> AHandler CheckSum
clientGetSum name remoteH = do
  maybe (0,"") id <$> clientRecvA (DSRGetSum name) remoteH
-- }}}

-- clientDup {{{
clientDup :: Bool -> String -> DServerInfo -> AHandler Bool
clientDup cache name target = do
  clientTwoStage req (const $ return True)
  where
    req = (if cache then DSRDupCache else DSRDupFile) name target
-- }}}

-- clientCmdH {{{

clientCmdH :: [String] -> AHandler (Either Bool [String])
-- Left
clientCmdH ("put":name:filename:[]) =
  (Left <$>) . clientPutFile False name filename
clientCmdH ("putc":name:filename:[]) =
  (Left <$>) . clientPutFile True name filename
clientCmdH ("get":name:filename:[]) =
  (Left <$>) . clientGetFile False name filename
clientCmdH ("getc":name:filename:[]) =
  (Left <$>) . clientGetFile True name filename
clientCmdH ("del":name:[]) =
  (Left <$>) . clientDel False name
clientCmdH ("delc":name:[]) =
  (Left <$>) . clientDel True name
clientCmdH ("freeze":name:[]) =
  (Left <$>) . clientFreeze name
clientCmdH ("freeze":[]) =
  (Left <$>) . clientFreezeAll
clientCmdH ("backup":[]) =
  (Left <$>) . clientBackup
clientCmdH ("verify":name:[]) =
  (Left <$>) . clientVerify name Nothing
clientCmdH ("verify":name:size:sum:[]) =
  (Left <$>) . clientVerify name (Just (read size, sum))
clientCmdH ("dup":name:tHost:tPort:[]) =
  (Left <$>) . clientDup False name serverinfo
  where
    serverinfo = DServerInfo tHost (read tPort) storageServiceType
clientCmdH ("dupc":name:tHost:tPort:[]) =
  (Left <$>) . clientDup True name serverinfo
  where
    serverinfo = DServerInfo tHost (read tPort) storageServiceType
-- Right
clientCmdH ("ls":[]) =
  (Right . map show . Map.toList <$>) . clientList False
clientCmdH ("lsc":[]) =
  (Right . map show . Map.toList <$>) . clientList True
clientCmdH ("cache":[]) =
  (Right . return . show <$>) . clientCacheSize
clientCmdH ("getsum":name:[]) =
  (Right . return . show <$>) . clientGetSum name
-- XX
clientCmdH cmd =
  const $ (putStrLn $ "?: " ++ show cmd) >> (return $ Left False)
-- }}}

-- listOnlineStorage {{{
listOnlineStorage :: ZKInfo -> IO [DServerInfo]
listOnlineStorage zkinfo = do
  mbList <- listServer zkinfo
  case mbList of
    Just lst -> return $ filter isStorage lst
    _ -> return []
  where
    isStorage dsi = dsiServType dsi == storageServiceType
-- }}}

-- }}}

-- user {{{

-- runSimpleServer {{{
runStorSimpleServer :: ZKInfo -> Integer -> FilePath -> IO ()
runStorSimpleServer zkinfo port rootpath = do
  commonInitial
  service <- makeDSService rootpath
  mbsd <- forkServer zkinfo service port
  maybe (return ()) waitCloseSignal mbsd
-- }}}

-- printHelp {{{
printHelp :: IO ()
printHelp = mapM_ putStrLn $
  [ "> quit"
  , "> refresh"
  , "> idup    <name> <from-ix> <to-ix>"
  , "> idupc   <name> <from-ix> <to-ix>"
  , "> * <rest-cmd>"
  , "> 0 <rest-cmd>"
  , "==========="
  , "rest-cmd::"
  , "> put     <name> <filename>"
  , "> putc    <name> <filename>"
  , "> get     <name> <filename>"
  , "> getc    <name> <filename>"
  , "> del     <name>"
  , "> delc    <name>"
  , "> freeze  <name>"
  , "> freeze"
  , "> backup"
  , "> verify  <name>"
  , "> verify  <name> <size> <checksum>"
  , "> dup     <name> <target-hostname> <target-port>"
  , "> dupc    <name> <target-hostname> <target-port>"
  , "> ls"
  , "> lsc"
  , "> cache"
  , "> getsum  <name>" ]
-- }}}

-- runClientREPL {{{
runStorClientREPL :: ZKInfo -> IO ()
runStorClientREPL zkinfo = do
  commonInitial
  lst <- listOnlineStorage zkinfo
  runLoopREPL zkinfo lst

runLoopREPL :: ZKInfo -> [DServerInfo] -> IO ()
runLoopREPL zkinfo lst = do
  putStrLn $ "==============================================================="
  putStrLn $ "==============================================================="
  --lst <- listOnlineStorage zkinfo
  putStrLn $ "current server list (" ++ show (length lst) ++ ") :"
  mapM_ (putStrLn . show) $ zip [(0 :: Integer) ..] lst
  printf "DStorage > " >> hFlush stdout
  cmd <- words <$> (getLine `catch` aHandler "quit")
  putStrLn $ "##cmd: " ++ show cmd
  if cmd == ["quit"]
  then putStrLn "bye!" >> hFlush stdout >> return ()
  else do
    if cmd == ["refresh"]
    then do
      lst' <- listOnlineStorage zkinfo
      runLoopREPL zkinfo lst'
    else do
      case cmd of
        ("h":[]) -> printHelp
        ("idup":cmd') -> runXDup False lst cmd'
        ("idupc":cmd') -> runXDup True lst cmd'
        ("*":cmd') -> mapM_ (\si -> accessAndPrint si cmd') lst
        (ixstr:cmd') -> do
          if all (`elem` "0123456789") ixstr
          then do
            let ix = read ixstr
            if ix >= length lst
            then putStrLn "invalid server ix"
            else accessAndPrint (lst !! ix) cmd'
          else do
            runNoIX cmd
        _ -> do
          runNoIX cmd
      runLoopREPL zkinfo lst
  where
    runNoIX cmd = do
      if length lst == 0
      then putStrLn "no server"
      else accessAndPrint (head lst) cmd
-- }}}

-- runXDup {{{
runXDup :: Bool -> [DServerInfo] -> [String] -> IO ()
runXDup cache lst (name:fromIxStr:toIxStr:[]) = do
  let fromIx = read fromIxStr
  let toIx = read toIxStr
  if fromIx >= length lst || toIx >= length lst
  then putStrLn "idup, Ix out of range"
  else do
    result <- accessServer (lst !! fromIx) $ clientDup cache name (lst !! toIx)
    putStrLn $ "dup: " ++ show result
runXDup _ _ _ = putStrLn "runXDup, wrong args"
-- }}}

-- accessAndPrint {{{
accessAndPrint :: DServerInfo -> [String] -> IO ()
accessAndPrint si args = do
  result <- accessServer si (clientCmdH args)
  case result of
    Just (Left ok) -> putStrLn $ "result: " ++ show ok
    Just (Right lst) -> putStrLn "result list: " >> mapM_ putStrLn lst
    _ -> putStrLn "accessServer failed"
-- }}}

-- }}}

-- vim: fdm=marker
