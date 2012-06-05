-- Copyright Wu Xingbo 2012

-- head {{{
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
-- }}}

-- module export {{{
module Eval.Storage (
  DStorConfig, DSReq(..), CheckSum,
  DSNode(..), DSDirInfo, DSFile(..), DSData,
  checkSumBS, checkSumBSL, checkSumFile, checkSumDSPath,
  uniqueName, openBinBufFile, getFileSize,
  pipeSome, pipeAll, toKey, fromKey,
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
                const, read, undefined,)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (Monad(..), mapM_, when, unless, void)
import Control.Concurrent (MVar, readMVar, modifyMVar_, newMVar,
                           withMVar, forkIO, modifyMVar,)
import Control.Exception (catch,)
--import Control.DeepSeq (deepseq)
import Data.Bits (shiftL, (.|.),)
import Data.Maybe (Maybe(..), maybe, isNothing)
import Text.Printf (printf)
import Data.Either (Either(..))
import Data.ByteString.UTF8 (fromString, toString,)
import System.IO (Handle, withFile, IOMode(..),
                  putStrLn, BufferMode(..), hSetBuffering,
                  openBinaryFile, hClose, hFlush, stdout, getLine)
import Data.Tuple (swap,)
import Data.List (map, filter, all, elem, (!!),
                  zip, words, length, head, sum,)
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
-- Key {{{
type Key = BS.ByteString
-- }}}
-- DStorConfig {{{
data DStorConfig = DStorConfig
  { confMetaData :: FilePath,
    confRootPath :: FilePath }
    --confHost       :: String }
  deriving (Show, Generic)
instance Serialize DStorConfig where
-- }}}
-- CheckSum {{{
type CheckSum = (Integer, BS.ByteString)
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
  = DSFile { dsfCheckSum :: CheckSum,
             dsfFilePath :: FilePath,
             dsfRWLock   :: RWL.RWLock }
  deriving (Generic)
instance Serialize DSFile where
  put (DSFile chksum path _) = put chksum >> put path
  get = DSFile <$> get <*> get <*> (return $ unsafePerformIO RWL.new)
instance Show DSFile where
  show (DSFile chksum path _) = "DSFile[" ++ show chksum ++ path ++ "]"
-- }}}
-- DSData {{{
data DSData
  = DSData { dsdCheckSum :: CheckSum,
             dsdData     :: BS.ByteString }
-- }}}
-- DSNode {{{
data DSNode = DSNode
  { nodeFileM   :: MVar (Map.Map Key DSFile),
    nodeCacheM  :: RWVar.RWVar (Map.Map Key DSData),
    nodeConfig  :: DStorConfig }
--type DSNodeStatic = Map.Map String DSFile
-- }}}
-- DSDirInfo {{{
type DSDirInfo = Map.Map Key CheckSum
-- }}}
-- DSReq {{{
-- req -> resp -> OP
data DSReq
  = DSRPutFile    Key CheckSum
  | DSRPutCache   Key CheckSum
  | DSRGetFile    Key
  | DSRGetCache   Key
  | DSRDelFile    Key
  | DSRDelCache   Key
  | DSRListFile
  | DSRListCache
  | DSRFreeze     Key
  | DSRFreezeAll
  | DSRBackup
  | DSRCacheSize
  | DSRVerify     Key (Maybe CheckSum) -- verify by checksum, delete on fail
  | DSRGetSum     Key
  | DSRDupFile    Key DServerInfo
  | DSRDupCache   Key DServerInfo
  deriving (Generic, Show)
instance Serialize DSReq where
-- }}}
-- }}}

-- basic helper {{{

-- readSHA1 {{{
readSHA1 :: String -> BS.ByteString
readSHA1 (a:b:rest) = BS.cons (read2 a b) $ readSHA1 rest
  where
  hexMap = Map.fromList $ [
    ('0',0), ('1',1), ('2',2), ('3',3), ('4',4),
    ('5',5), ('6',6), ('7',7), ('8',8), ('9',9),
    ('a',10),('b',11),('c',12),('d',13),('e',14),('f',15),
    ('A',10),('B',11),('C',12),('D',13),('E',14),('F',15)]
  read1 x = maybe undefined id $ Map.lookup x hexMap
  read2 x y = ((read1 x) `shiftL` 4) .|. (read1 y)
readSHA1 _ = BS.empty
-- }}}

-- checkSum {{{
checkSumBS :: BS.ByteString -> CheckSum
checkSumBS bs = (fromIntegral (BS.length bs), chksum)
  where !chksum = SHA1.hash bs

checkSumBSL :: BSL.ByteString -> CheckSum
checkSumBSL bs = (fromIntegral (BSL.length bs), chksum)
  where !chksum = SHA1.hashlazy bs

checkSumFile :: FilePath -> IO CheckSum
checkSumFile path = do
  h <- openBinBufFile path ReadMode
  checkSumBSL <$> BSL.hGetContents h

checkSumDSPath :: DSNode -> FilePath -> IO CheckSum
checkSumDSPath node path = do
  h <- openLocalFile node path ReadMode
  checkSumBS <$> BS.hGetContents h

verifyDSFile :: DSNode -> DSFile -> Maybe CheckSum -> IO Bool
verifyDSFile node dsfile mbchksum = do
  if maybe True (fsum ==) mbchksum
  then do
    ((fsum ==) <$> checkSumDSPath node fpath) `catch` aHandler False
  else return False
  where
    fsum = dsfCheckSum dsfile
    fpath = dsfFilePath dsfile

verifyDSData :: DSData -> Maybe CheckSum -> Bool
verifyDSData dsd mbchksum = do
  if maybe True (dsum ==) mbchksum
  then dsum == checkSumBS ddata
  else False
  where
    dsum = dsdCheckSum dsd
    ddata = dsdData dsd
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
  fullLocalFilePath node $ dsfFilePath dsfile
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

-- keyOP {{{
toKey :: String -> Key
toKey = fromString

fromKey :: Key -> String
fromKey = toString
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
cacheToDSFile :: DSNode -> DSData -> IO DSFile
cacheToDSFile node dsd = do
  path <- uniqueName
  to <- openLocalFile node path WriteMode
  BS.hPut to $ dsdData dsd
  lock <- RWL.new
  return $ DSFile (dsdCheckSum dsd) path lock
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
nodeRemoteReadFile :: DSNode -> Key -> Handle -> IO ()
nodeRemoteReadFile node name remoteH = do
  --putStrLn $ "remote read file: " ++ name
  mbdsfile <- nodeLookupFile node name
  case mbdsfile of
    Just dsfile -> RWL.withRead (dsfRWLock dsfile) $ do
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
nodeRemoteReadCache :: DSNode -> Key -> Handle -> IO ()
nodeRemoteReadCache node name remoteH = do
  mbentry <- nodeLookupCache node name
  case mbentry of
    Just entry -> do
      respOK remoteH
      putObject remoteH $ dsdCheckSum entry
      BS.hPut remoteH $ dsdData entry
      respOK remoteH
    _ -> do
      nodeRemoteReadFile node name remoteH
      void $ forkIO $ nodeLocalCacheFile node name
-- }}}

-- nodeRemoteWriteFile {{{
nodeRemoteWriteFile :: DSNode -> Key -> CheckSum -> Handle -> IO ()
nodeRemoteWriteFile node name chksum remoteH = do
  respOK remoteH
  nodeLocalDeleteCache node name
  nodeLocalDeleteFile node name
  --putStrLn "start pipe to DSFile"
  mbdsfile <- pipeToDSFile node (fst chksum) remoteH
  --putStrLn "finish pipe to DSFile"
  case mbdsfile of
    Just dsfile -> do
      modifyMVar_ (nodeFileM node) $ return . Map.insert name dsfile
      respOK remoteH
    _ -> respFail remoteH
-- }}}

-- nodeRemoteWriteCache {{{
nodeRemoteWriteCache :: DSNode -> Key -> CheckSum -> Handle -> IO ()
nodeRemoteWriteCache node key chksum remoteH = do
  respOK remoteH
  --putStrLn "write cache started"
  bs <- BS.hGet remoteH (fromIntegral $ fst chksum)
  --putStrLn "write cache finished"
  RWVar.modify_ (nodeCacheM node) $ return . Map.insert key (DSData chksum bs)
  respOK remoteH
  --void $ forkIO $ nodeLocalDumpFile node name
-- }}}

-- nodeRemoteDeleteFile {{{
nodeRemoteDeleteFile :: DSNode -> Key -> Handle -> IO ()
nodeRemoteDeleteFile node name remoteH = do
  nodeLocalDeleteFile node name
  respOK remoteH
-- }}}

-- nodeRemoteDeleteCache {{{
nodeRemoteDeleteCache :: DSNode -> Key -> Handle -> IO ()
nodeRemoteDeleteCache node name remoteH = do
  nodeLocalDeleteCache node name
  respOK remoteH
-- }}}

-- nodeRemoteListFile {{{
nodeRemoteListFile :: DSNode -> Handle -> IO ()
nodeRemoteListFile node remoteH = do
  respOK remoteH
  filemap <- readMVar (nodeFileM node)
  putObject remoteH $ Map.map dsfCheckSum filemap
  respOK remoteH
-- }}}

-- nodeRemoteListCache {{{
nodeRemoteListCache :: DSNode -> Handle -> IO ()
nodeRemoteListCache node remoteH = do
  respOK remoteH
  cachemap <- RWVar.with (nodeCacheM node) $ return
  putObject remoteH (Map.map dsdCheckSum cachemap :: DSDirInfo)
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
nodeRemoteVerify :: DSNode -> Key -> (Maybe CheckSum) -> Handle -> IO ()
nodeRemoteVerify node name mbsum remoteH = do
  respOK remoteH
  ok <- nodeLocalVerify node name mbsum
  putObject remoteH ok
  respOK remoteH
-- }}}

-- nodeRemoteGetSum {{{
nodeRemoteGetSum :: DSNode -> Key -> Handle -> IO ()
nodeRemoteGetSum node name remoteH = do
  mbdsfile <- nodeLookupFile node name
  case mbdsfile of
    Just dsfile -> do
      respOK remoteH
      putObject remoteH (dsfCheckSum dsfile)
      respOK remoteH
    _ -> do
      mbentry <- nodeLookupCache node name
      case mbentry of
        Just entry -> do
          respOK remoteH
          putObject remoteH $ dsdCheckSum entry
          respOK remoteH
        _ -> respFail remoteH
-- }}}

-- nodeRemoteDupFile {{{
nodeRemoteDup :: Bool -> DSNode -> Key -> DServerInfo -> Handle -> IO ()
nodeRemoteDup cache node key target remoteH = do
  respOK remoteH
  mbentry <- nodeLookupCache node key
  case mbentry of
    Just entry -> do
      mbok <- accessServer target (clientPutBS cache key (dsdData entry))
      case mbok of
        Just True -> respOK remoteH
        _ -> respFail remoteH
    _ -> do
      mbdsfile <- nodeLookupFile node key
      case mbdsfile of
        Just dsfile -> do
          localH <- openDSFile node dsfile ReadMode
          let chksum = dsfCheckSum dsfile
          mbok <- accessServer target (clientPutH cache key localH chksum)
          case mbok of
            Just True -> respOK remoteH
            _ -> respFail remoteH
        _ -> respFail remoteH
-- }}}

-- local basic
-- nodeLookupFile {{{
nodeLookupFile :: DSNode -> BS.ByteString -> IO (Maybe DSFile)
nodeLookupFile node name = do
  withMVar (nodeFileM node) $ return . Map.lookup name
-- }}}

-- nodeLookupCache {{{
nodeLookupCache :: DSNode -> Key -> IO (Maybe DSData)
nodeLookupCache node name = do
  RWVar.with (nodeCacheM node) $ return . Map.lookup name
-- }}}

-- nodeLocalMemoryUsage {{{
nodeLocalMemoryUsage :: DSNode -> IO Integer
nodeLocalMemoryUsage node = do
  cacheM <- RWVar.with (nodeCacheM node) return
  return $ sum $ map (fst . dsdCheckSum) $ (Map.elems cacheM)
-- }}}

-- local advanced
-- nodeLocalDumpFile {{{
nodeLocalDumpFile :: DSNode -> Key -> IO ()
nodeLocalDumpFile node name = do
  mbdsfile <- nodeLookupFile node name
  when (isNothing mbdsfile) $ do
    mbentry <- nodeLookupCache node name
    case mbentry of
      Just entry -> do
        dsfile <- cacheToDSFile node entry
        modifyMVar_ (nodeFileM node) $ return . Map.insert name dsfile
      _ -> return ()
-- }}}

-- nodeLocalFreezeData {{{
nodeLocalFreezeData :: DSNode -> Key -> IO ()
nodeLocalFreezeData node name = do
  nodeLocalDumpFile node name
  nodeLocalDeleteCache node name
-- }}}

-- nodeLocalCacheFile {{{
nodeLocalCacheFile :: DSNode -> Key -> IO ()
nodeLocalCacheFile node name = do
  mbbs <- nodeLookupCache node name
  when (isNothing mbbs) $ do
    mbdsfile <- nodeLookupFile node name
    case mbdsfile of
      Just dsfile -> do
        entry <- RWL.withRead (dsfRWLock dsfile) $ do
          bs <- loadDSFileToBS node dsfile
          return $ DSData (dsfCheckSum dsfile) bs
        RWVar.modify_ (nodeCacheM node) $ return . Map.insert name entry
      _ -> return ()
-- }}}

-- nodeLocalBackupMeta {{{
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
nodeLocalDeleteFile :: DSNode -> Key -> IO ()
nodeLocalDeleteFile node name = do
  mbdsfile <- modifyMVar (nodeFileM node) $
    return . swap . Map.updateLookupWithKey (const . const $ Nothing) name
  case mbdsfile of
    Just dsfile -> do
      RWL.withWrite (dsfRWLock dsfile) $ do
      deleteDSFile node dsfile
    _ -> return ()
-- }}}

-- nodeLocalDeleteCache {{{
nodeLocalDeleteCache :: DSNode -> Key -> IO ()
nodeLocalDeleteCache node key = do
  RWVar.modify_ (nodeCacheM node) $ return . Map.delete key
-- }}}

-- nodeLocalVerify {{{
nodeLocalVerifyFile :: DSNode -> Key -> Maybe CheckSum -> IO (Maybe Bool)
nodeLocalVerifyFile node key mbsum = do
  mbdsfile <- nodeLookupFile node key
  mbdsfileOK <- case mbdsfile of
    Just dsfile -> Just <$> verifyDSFile node dsfile mbsum
    _ -> return Nothing
  when (mbdsfileOK == Just False) $ do
    nodeLocalDeleteFile node key
  return mbdsfileOK

nodeLocalVerifyCache :: DSNode -> Key -> Maybe CheckSum -> IO (Maybe Bool)
nodeLocalVerifyCache node key mbsum = do
  mbentry <- nodeLookupCache node key
  cacheOK <- case mbentry of
    Just entry -> return $ Just $ verifyDSData entry mbsum
    _ -> return Nothing
  when (cacheOK == Just False) $ do
    nodeLocalDeleteCache node key
  return cacheOK

nodeLocalVerify :: DSNode -> Key -> Maybe CheckSum -> IO Bool
nodeLocalVerify node name mbchksum = do
  mbdsfileOK <- nodeLocalVerifyFile node name mbchksum
  mbcacheOK <- nodeLocalVerifyCache node name mbchksum
  --putStrLn $ "localverify:" ++ show (mbdsfileOK, mbcacheOK)
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
  --putStrLn $ "recv request:" ++ show mbReq
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
    (DSRPutFile name chksum) -> do
      --putStrLn $ "processing PutFile"
      if fst chksum > 0x10000000
      then respFail remoteH
      else respDo $ nodeRemoteWriteFile node name chksum remoteH
    (DSRPutCache name chksum) -> if fst chksum > 0x10000000
      then respFail remoteH
      else respDo $ nodeRemoteWriteCache node name chksum remoteH
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
clientPutFile :: Bool -> Key -> FilePath -> AHandler Bool
clientPutFile cache name filepath remoteH = do
  --putStrLn $ "start get chksum:"
  chksum <- checkSumFile filepath
  --putStrLn $ "sum:" ++ show chksum
  clientTwoStage (req chksum) putData remoteH
  where
    req chksum = (if cache then DSRPutCache else DSRPutFile) name chksum
    putData rH = do
      --putStrLn "start send file content"
      openBinBufFile filepath ReadMode >>= flip pipeAll rH
      --putStrLn "finish send file content"
      return True
-- }}}

-- clientPutBS {{{
clientPutBS :: Bool -> Key -> BS.ByteString -> AHandler Bool
clientPutBS cache name bs remoteH = do
  clientTwoStage req putData remoteH
  where
    chksum = checkSumBS bs
    req = (if cache then DSRPutCache else DSRPutFile) name chksum
    putData rH = BS.hPut rH bs >> return True
-- }}}

-- clientPutH {{{
clientPutH :: Bool -> Key -> Handle -> CheckSum -> AHandler Bool
clientPutH cache name from chksum remoteH = do
  clientTwoStage req pipeData remoteH
  where
    req = (if cache then DSRPutCache else DSRPutFile) name chksum
    pipeData rH = pipeSome (fst chksum) from rH >> return True
-- }}}

-- clientGetFile {{{
clientGetFile :: Bool -> Key -> FilePath -> AHandler Bool
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
clientGetH :: Bool -> Key -> Handle -> AHandler Bool
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
clientDel :: Bool -> Key -> AHandler Bool
clientDel cache name = clientNoRecv $
  if cache then DSRDelCache name else DSRDelFile name

clientFreeze :: Key -> AHandler Bool
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
clientVerify :: Key -> Maybe CheckSum -> AHandler Bool
clientVerify name mbchecksum remoteH = do
  maybe False id <$> clientRecvA (DSRVerify name mbchecksum) remoteH
-- }}}

-- clientGetSum {{{
clientGetSum :: Key -> AHandler CheckSum
clientGetSum name remoteH = do
  maybe (0,BS.empty) id <$> clientRecvA (DSRGetSum name) remoteH
-- }}}

-- clientDup {{{
clientDup :: Bool -> Key -> DServerInfo -> AHandler Bool
clientDup cache name target = do
  clientTwoStage req (const $ return True)
  where
    req = (if cache then DSRDupCache else DSRDupFile) name target
-- }}}

-- clientCmdH {{{

clientCmdH :: [String] -> AHandler (Either Bool [String])
-- Left
clientCmdH ("put":key:filename:[]) =
  (Left <$>) . clientPutFile False (toKey key) filename
clientCmdH ("putc":key:filename:[]) =
  (Left <$>) . clientPutFile True (toKey key) filename
clientCmdH ("get":key:filename:[]) =
  (Left <$>) . clientGetFile False (toKey key) filename
clientCmdH ("getc":key:filename:[]) =
  (Left <$>) . clientGetFile True (toKey key) filename
clientCmdH ("del":key:[]) =
  (Left <$>) . clientDel False (toKey key)
clientCmdH ("delc":key:[]) =
  (Left <$>) . clientDel True (toKey key)
clientCmdH ("freeze":key:[]) =
  (Left <$>) . clientFreeze (toKey key)
clientCmdH ("freeze":[]) =
  (Left <$>) . clientFreezeAll
clientCmdH ("backup":[]) =
  (Left <$>) . clientBackup
clientCmdH ("verify":key:[]) =
  (Left <$>) . clientVerify (toKey key) Nothing
clientCmdH ("verify":key:size:sumstr:[]) =
  (Left <$>) . clientVerify (toKey key) (Just (read size, readSHA1 sumstr))
clientCmdH ("dup":key:tHost:tPort:[]) =
  (Left <$>) . clientDup False (toKey key) serverinfo
  where
    serverinfo = DServerInfo tHost (read tPort) storageServiceType
clientCmdH ("dupc":key:tHost:tPort:[]) =
  (Left <$>) . clientDup True (toKey key) serverinfo
  where
    serverinfo = DServerInfo tHost (read tPort) storageServiceType
-- Right
clientCmdH ("ls":[]) =
  (Right . map show . Map.toList <$>) . clientList False
clientCmdH ("lsc":[]) =
  (Right . map show . Map.toList <$>) . clientList True
clientCmdH ("cache":[]) =
  (Right . return . show <$>) . clientCacheSize
clientCmdH ("getsum":key:[]) =
  (Right . return . show <$>) . clientGetSum (toKey key)
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
    result <- accessServer (lst !! fromIx) $
                clientDup cache (toKey name) (lst !! toIx)
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
