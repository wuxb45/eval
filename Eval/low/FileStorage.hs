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
------
import Prelude (($), (.), (+), (>), (==),
                flip, Bool(..), compare, fst, fromIntegral,
                IO, String, Show(..), Integer, id, )
import Control.Applicative ((<$>), )
import Control.Monad (Monad(..), mapM,)
import Control.Concurrent (MVar, readMVar, modifyMVar_, newMVar,
                           putMVar, takeMVar,)
import Control.Exception (catch, IOException, SomeException)
import Control.DeepSeq (deepseq)
import Data.Maybe (Maybe(..), maybe, catMaybes,)
import Text.Printf (printf)
import Data.Word (Word64)
import Data.Int (Int32, Int64)
import Data.IORef (IORef(..), readIORef)
import System.IO (Handle, withFile, IOMode(..), withBinaryFile,)
import System.IO.Error (userError, IOError, ioError)
import System.FilePath (FilePath)
import System.Directory (removeFile)
import System.Posix.Time (epochTime)
import System.Posix.Process (getProcessID)
import System.Posix.Types (CPid(..))
import System.Time (ClockTime, getClockTime)
import Foreign.C.Types (CTime(..))
import Data.List (concatMap, map, sortBy)
import GHC.Generics (Generic)
import Data.Serialize (Serialize(..), encode, decode)
import System.CPUTime.Rdtsc (rdtsc)
------
import Eval.DServ (IOHandler, DService(..), DServerData, forkServer,
                   closeServer, putObject, getObject)
-- }}}

-- data {{{
-- StorageConfig {{{
data StorageConfig = SConf
  { sconfMetaData   :: FilePath,
    sconfRootPath   :: FilePath,
    sconfCacheSize  :: Integer,
    sconfHost       :: String }
  deriving (Show, Generic)
instance Serialize StorageConfig where
-- }}}
-- StorageCell {{{
type CheckSum = (Integer, String)
type StorageCell = MVar (StorageData, CheckSum, StorageAccessLog)
-- }}}
-- StorageAccessLog {{{
data StorageAccessLog = SALog [StorageAccess] deriving (Show, Generic)
data StorageAccess = SA
  { saClockTime :: ClockTime,
    saStatus    :: StorageDataInfo,
    saMessage   :: String }
  deriving (Show, Generic)
data StorageDataInfo
  = Full      -- ^ Both
  | Hot       -- ^ only in memory
  | Freezing  -- ^ on disk
  | Dead      -- ^ noData
  deriving (Show, Generic)
-- }}}
-- StorageData {{{
data StorageData
  = SDDisk FilePath
  | SDMemory BS.ByteString
  | SDBoth FilePath BS.ByteString
  | SDNothing
  deriving (Generic)
instance Serialize StorageData where
-- }}}
-- StorageNode {{{
data StorageNode = SNode
  { snodeMap    :: MVar (Map.Map String StorageCell),
    snodeCache  :: MVar [String], -- take map, then take recent
    snodeConfig :: StorageConfig }
-- }}}
-- DStorageRequest {{{
data DStorageRequest
  = DSRPut     String
  | DSRGet     String
  | DSRDrop    String
  | DSRPreload String
  | DSRFlushAll
  deriving (Generic, Show)
instance Serialize DStorageRequest where
-- }}}
-- DStorageResponse {{{
data DStorageResponse = DSRFail | DSROkay deriving (Generic, Show)
instance Serialize DStorageResponse where
-- }}}
-- }}}

-- checkSum {{{
checkSumBS :: BS.ByteString -> CheckSum
checkSumBS bs = sum `deepseq` (fromIntegral (BS.length bs), sum)
  where sum = concatMap (printf "%02x") $ BS.unpack $ SHA1.hash bs

checkSumFile :: FilePath -> IO CheckSum
checkSumFile path = do
  bs <- withBinaryFile path ReadMode BS.hGetContents
  return $ checkSumBS bs
-- }}}

-- uniqueName {{{
uniqueName :: IO FilePath
uniqueName = do
  (CTime clocktime) <- epochTime   -- Int64
  tick64 <- rdtsc  -- Word64
  (CPid pid) <- getProcessID -- Int32
  --let hashvalue = checkSumBS bs
  return $ printf "%016x-%016x-%08x" clocktime tick64 pid
-- }}}

-- StorageData {{{

-- sdStatus {{{
sdStatus :: StorageData -> StorageDataInfo
sdStatus (SDBoth _ _) = Full
sdStatus (SDMemory _) = Hot
sdStatus (SDDisk _) = Freezing
sdStatus (SDNothing) = Dead
-- }}}

-- IO between BS & File (throws exceptions) {{{
writeBSToDisk :: BS.ByteString -> IO FilePath
writeBSToDisk bs = do
  path <- uniqueName
  BS.writeFile path bs
  return path

loadFileToBS :: FilePath -> IO BS.ByteString
loadFileToBS = BS.readFile
-- }}}

-- StorageData Sync {{{
aHandler :: a -> IOException -> IO a
aHandler a e = return a

-- when failed to write file, keep it in memory
sdSyncToDisk :: StorageData -> IO StorageData
sdSyncToDisk orig@(SDMemory bs) =
  (flip SDBoth bs <$> writeBSToDisk bs) `catch` aHandler orig
sdSyncToDisk sd = return sd

-- when failed to load file, drop file-path
sdSyncToMemory :: StorageData -> IO StorageData
sdSyncToMemory (SDDisk path) =
  (SDBoth path <$> loadFileToBS path) `catch` aHandler SDNothing
sdSyncToMemory sd = return sd

-- drop
sdDropMemory :: StorageData -> IO StorageData
sdDropMemory (SDMemory _) = return $ SDNothing
sdDropMemory (SDBoth path _) = return $ SDDisk path
sdDropMemory sd = return sd

sdDropFile :: StorageData -> IO StorageData
sdDropFile (SDDisk path) = do
  removeFile path `catch` aHandler ()
  return $ SDNothing
sdDropFile (SDBoth path bs) = do
  removeFile path `catch` aHandler ()
  return $ SDMemory bs
sdDropFile sd = return sd

-- write to file and empty memory, drop when failed to write.
sdFlush :: StorageData -> IO StorageData
sdFlush sd = sdSyncToDisk sd >>= sdDropMemory

{-
sdGetData :: StorageData -> IO BS.ByteString
sdGetData sd = do
  sdOK <- sdSyncToMemory sd 
  case sdOK of
    SDMemory bs -> return bs
    SDBoth _ bs -> return bs
    SDDisk bs -> ioError $ userError "Cannot get data from StorageData."
-}
-- }}}

-- sdPeakBS {{{
sdPeakBS :: StorageData -> Maybe BS.ByteString
sdPeakBS (SDMemory bs) = Just bs
sdPeakBS (SDBoth _ bs) = Just bs
sdPeakBS _ = Nothing
-- }}}

-- sdPeakFilePath {{{
sdPeakFilePath :: StorageData -> Maybe FilePath
sdPeakFilePath (SDDisk path) = Just path
sdPeakFilePath (SDBoth path _) = Just path
sdPeakFilePath _ = Nothing
-- }}}

-- sdMemoryUsage {{{
sdMemoryUsage :: StorageData -> Integer
sdMemoryUsage (SDMemory bs) = fromIntegral $ BS.length bs
sdMemoryUsage (SDBoth _ bs) = fromIntegral $ BS.length bs
sdMemoryUsage _ = 0
-- }}}

-- }}}

-- StorageAccessLog {{{
saLogAppend :: StorageAccessLog -> StorageData -> String -> IO StorageAccessLog
saLogAppend (SALog lst) sd msg = do
  t <- getClockTime
  return $ SALog $ (SA t (sdStatus sd) msg):lst
-- }}}

-- StorageCell {{{

-- scellNew {{{
scellNew :: String -> BS.ByteString -> IO StorageCell
scellNew name bs = do
  newMVar (SDMemory bs, checkSumBS bs, SALog [])
-- }}}

-- scellRead {{{
-- slow operation!
scellRead :: StorageCell -> IO (Maybe BS.ByteString)
scellRead sc = do
  mbbs <- scellReadCached sc
  scellFreeze sc
  return mbbs
-- }}}

-- scellReadCached {{{
-- slow operation!
scellReadCached :: StorageCell -> IO (Maybe BS.ByteString)
scellReadCached sc = do
  (sd, chk, sa) <- takeMVar sc
  case sdPeakBS sd of
    okBS@(Just _) -> do -- best case: online
      logHot <- saLogAppend sa sd "read, hot"
      putMVar sc (sd, chk, logHot)
      return okBS
    _ -> do
      sd' <- sdSyncToMemory sd
      case sdPeakBS sd' of
        okBS'@(Just _) -> do -- ok, sync to memory
          logCold <- saLogAppend sa sd' "read, cold"
          putMVar sc (sd', chk, logCold)
          return okBS'
        _ -> do
          logDead <- saLogAppend sa sd' "read, dead"
          putMVar sc (sd', chk, logDead)
          return Nothing
-- }}}

-- scellDropBS {{{
scellDropMemory :: StorageCell -> IO ()
scellDropMemory sc = do
  (sd, chk, sa) <- takeMVar sc
  sd' <- sdDropMemory sd
  sa' <- saLogAppend sa sd' "drop memory"
  putMVar sc (sd', chk, sa')
-- }}}

-- scellDropFile {{{
scellDropFile :: StorageCell -> IO ()
scellDropFile sc = do
  (sd, chk, sa) <- takeMVar sc
  sd' <- sdDropFile sd
  sa' <- saLogAppend sa sd' "drop file"
  putMVar sc (sd', chk, sa')
-- }}}

-- scellWrite {{{
scellWrite :: StorageCell -> BS.ByteString -> IO ()
scellWrite sc bs = do
  scellWriteCached sc bs
  scellFreeze sc
-- }}}

-- scellWriteCached {{{
-- slow !
scellWriteCached :: StorageCell -> BS.ByteString -> IO ()
scellWriteCached sc bs = do
  let !checksum = checkSumBS bs
  let newsd = SDMemory bs
  scellDropFile sc
  scellDropMemory sc
  (_, _, sa) <- takeMVar sc
  sa' <- saLogAppend sa newsd "write memory"
  putMVar sc (SDMemory bs, checksum, sa')
-- }}}

-- scellCheckMemory {{{
scellCheckMemory :: StorageCell -> IO ()
scellCheckMemory sc = do
  (sd, chk, sa) <- takeMVar sc
  case sdPeakBS sd of
    Just bs -> do
      if chk == checkSumBS bs
      then do
        sa' <- saLogAppend sa sd "check memory, ok"
        putMVar sc (sd, chk, sa')
      else do
        sd' <- sdDropMemory sd
        sa' <- saLogAppend sa sd' "check memory, fail"
        putMVar sc (sd', chk, sa')
    _ -> do
      sa' <- saLogAppend sa sd "check memory, offline"
      putMVar sc (sd, chk, sa')
-- }}}

-- scellCheckFile {{{
scellCheckFile :: StorageCell -> IO ()
scellCheckFile sc = do
  (sd, chk, sa) <- takeMVar sc
  case sdPeakFilePath sd of
    Just path -> do
      chksum <- checkSumFile path `catch` aHandler []
      if chksum == chk
      then do
        sa' <- saLogAppend sa sd "check file, ok"
        putMVar sc (sd, chk, sa')
      else do
        sd' <- sdDropFile sd
        sa' <- saLogAppend sa sd' "check file, fail"
        putMVar sc (sd', chk, sa')
    _ -> do
      sa' <- saLogAppend sa sd "check file, nofile"
      putMVar sc (sd, chk, sa')
-- }}}

-- scellMemoryUsage {{{
scellMemoryUsage :: StorageCell -> IO Integer
scellMemoryUsage sc = do
  (sd, sum, sa) <- readMVar sc
  return $ sdMemoryUsage sd
-- }}}

-- scellFreeze {{{
scellFreeze :: StorageCell -> IO ()
scellFreeze sc = do
  (sd, chk, sa) <- takeMVar sc
  sd' <- sdFlush sd
  sa' <- saLogAppend sa sd' "freeze cell"
  putMVar sc (sd', chk, sa')
-- }}}

-- }}}

-- StorageNode {{{

-- snMemoryUsage {{{
snMemoryUsage :: StorageNode -> IO Integer
snMemoryUsage sn = do
  cellmap <- readMVar (snodeMap sn)
  foldM plus 0 $ Map.elems cellmap
  where
    plus orig cell = (orig +) <$> (scellMemoryUsage cell)
-- }}}

-- snUpdateCache {{{
snUpdateCache :: StorageNode -> IO StorageNode
snUpdateCache sn = do
  cacheL <- takeMVar (snodeCache sn)
  let (keepL, dropL) = splitAt cacheSize (nub cacheL)
  mapM (uncache cellmap) dropL
  putMVar (snodeMap sn) (cellmap, keepL)
  where
    cacheSize = fromIntegral $ sconfMemorySize $ snodeConfig sn
    uncache cellmap name = do
      maybe (return ()) scellFreeze $ Map.lookup name cellmap
-- }}}

-- snTouchCache {{{
snTouchCache :: String -> MVar [String] -> IO ()
snTouchCache name mvar = modifyMVar_ mvar $ (name:) . filter (/= name)
-- }}}

-- snRead {{{
-- read and drop cache
snRead :: String -> StorageNode -> IO (Maybe BS.ByteString)
snRead name sn = do
  cellmap <- readMVar (snodeMap sn)
  case Map.lookup name cellmap of
    Just cell -> scellRead cell
    _ -> return Nothing
-- }}}

-- snReadCached {{{
snReadCached :: String -> StorageNode -> IO (Maybe BS.ByteString)
snReadCached name sn = do
  cacheL <- takeMVar (snodeCache sn)
  case Map.lookup name cellmap of
    Just cell -> do
      mbbs <- scellReadCached cell
      case mbbs of
        Just bs -> do
          putMVar (snodeCache sn) (name:(filter (/= name) cacheL))
        _ -> do
          putMVar (snodeCache sn) cacheL
      return mbbs
    _ -> do
      putMVar (snodeCache sn) cacheL
      return Nothing
-- }}}

-- snWrite {{{
snWrite :: String -> StorageNode -> BS.ByteString -> IO ()
snWrite name sn bs = do
  snWriteCached name sn bs
  snFreeze name sn
-- }}}

-- snWriteCached {{{
snWriteCached :: String -> StorageNode -> BS.ByteString -> IO ()
snWriteCached name sn bs = do
  cellmap <- readMVar (snodeMap sn)
  case Map.lookup name cellmap of
    Just cell -> scellWrite cell bs
    _ -> do
      cacheL <- takeMVar (snodeCache sn)
      cellmap <- takeMVar (snodeMap sn)
      cell <- scellNew name bs
      putMVar (snodeMap sn) $ Map.insert name cell cellmap
-- }}}

-- }}}

-- config file {{{
sconfLoadFile :: FilePath -> IO (Maybe StorageConfig)
sconfLoadFile path = do
  withFile path ReadMode getObject `catch` exHandler
  where
    exHandler (e :: IOException) = return Nothing
-- }}}

-- make service {{{
makeStorageService :: FilePath -> IO (Maybe DService)
makeStorageService confpath = do
  mbConf <- sconfLoadFile confpath
  case mbConf of
    Just conf -> do
      mvar <- newMVar (SN Map.empty conf)
      return $ Just $ DService "DStorage" (storageHandler mvar)
    _ -> return Nothing
-- }}}

-- main handler {{{
storageHandler :: (MVar StorageNode) -> IOHandler
storageHandler sn h = do
  mbReq <- getObject h
  case mbReq of
    Just req -> handleReq sn h req
    _ -> putObject h DSRFail
-- }}}

-- request handler {{{
handleReq :: (MVar StorageNode) -> Handle -> DStorageRequest -> IO ()
handleReq mvar h (DSRGet name) = do
  sn <- readMVar mvar
  mbbs <- snLookupBS name sn
  case mbbs of
    Just bs -> putObject h DSROkay >> putObject h bs
    _ -> putObject h DSRFail

handleReq mvar h (DSRPut name) = do
  mbObj <- getObject h
  case mbObj of
    Just bs -> do
      modifyMVar_ mvar $ return . snUpdateData name (SDMemory bs)
      putObject h DSROkay
    _ -> putObject h DSRFail

handleReq mvar h (DSRDrop name) = do
  modifyMVar_ mvar $ return . snDeleteData name
  putObject h DSROkay -- no fail, hint?

handleReq mvar h (DSRPreload name) = do
  modifyMVar_ mvar $ snPreload name
  putObject h DSROkay -- no fail, hint?

handleReq mvar h DSRFlushAll = do
  modifyMVar_ mvar $ snFlush
-- }}}

-- }}}

-- vim: fdm=marker
