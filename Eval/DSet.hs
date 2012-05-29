-- Copyright 2012 Wu Xingbo
-- head {{{
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
-- }}}

-- module export {{{
module Eval.DSet where
-- }}}

-- import {{{
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified Data.Array as Arr
import qualified Data.Traversable as Trav
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Crypto.Hash.SHA1 as SHA1 -- hackage: cryptohash
import qualified Control.Concurrent.ReadWriteVar as RWVar
import qualified Control.Concurrent.ReadWriteLock as RWL
------
import Prelude (($), (.), (+), (>), (==), (++), (-), (/=), (||), (&&),
                flip, Bool(..), compare, fromIntegral, Int,
                IO, String, Show(..), Integer, id, toInteger,
                const, read, snd, mod,)
import Control.Applicative ((<$>), (<*>))
import Control.Monad (Monad(..), mapM_, when, unless, void, mapM, join)
import Control.Concurrent (MVar, readMVar, modifyMVar_, newMVar,
                           withMVar, forkIO, modifyMVar,)
import Control.Exception (catch,)
import Control.DeepSeq (deepseq)
import Data.Set (Set)
import Data.Array.IArray
import Data.Map (Map)
import Data.Word (Word64)
import Data.Maybe (Maybe(..), maybe, isNothing)
import Text.Printf (printf)
import Data.Either (Either(..))
import System.IO (Handle, withFile, IOMode(..), withBinaryFile,
                  putStrLn, BufferMode(..), hSetBuffering,
                  openBinaryFile, hClose)
import Data.Tuple (swap,)
import Data.List (foldl', concatMap, map, filter, zip, foldr, length, sum)
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
import Eval.DServ
import Eval.Storage
-- }}}

-- data {{{
-- DFSConfig {{{
data DSetConfig = DSetConfig
  { confMetaData :: FilePath,
    confZKInfo   :: ZKInfo }
-- }}}
-- DataSet {{{
type DataSet = Map String CheckSum
-- }}}
-- DSetNode {{{
type DSetMetaMap = Map String (RWVar.RWVar DataSet)
-- (_, files, caches)
type StorageInfo = (DServerInfo, DSDirInfo, DSDirInfo)

data DSetNode = DSetNode
  { dsetMetaMap     :: RWVar.RWVar DSetMetaMap,
    dsetStorage     :: MVar (Array Int StorageInfo),
    dsetSeed        :: MVar Int,
    dsetConfig      :: DSetConfig }
-- }}}
-- DSetReq {{{
data DSetReq
  = DSetRAddElemN    String [(String, CheckSum)]
  | DSetRDelElemN    String [String]
  | DSetRDelAll      String
  | DSetRGetSet      String -- returns DataSet
  | DSetRPickStorR   String -- returns (Maybe DServerInfo)
  | DSetRPickStorW
  deriving (Show, Generic)
instance Serialize DSetReq where
-- }}}
-- }}}

-- DSet {{{

-- dsetLookup {{{
dsetLookup :: DSetNode -> String -> IO DataSet
dsetLookup node sname = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  maybe (return Map.empty) (\s -> RWVar.with s return) mbset
-- }}}

-- dsetAddElem {{{
--dsetAddElem :: DSetNode -> String -> String -> CheckSum -> IO ()
--dsetAddElem node sname ename sum = do
--  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
--  case mbset of
--    Just set -> do
--      RWVar.modify_ set $ return . Map.insert ename sum
--    _ -> do
--      set <- RWVar.new $ Map.singleton ename sum
--      RWVar.modify_ (dsetMetaMap node) $ return . Map.insert sname set
-- }}}

-- dsetAddElemN {{{
dsetAddElemN :: DSetNode -> String -> [(String, CheckSum)] -> IO ()
dsetAddElemN node sname pairs = do
  let newMap = Map.fromList pairs
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.union newMap
    _ -> do
      set <- RWVar.new newMap
      RWVar.modify_ (dsetMetaMap node) $ return . Map.insert sname set
-- }}}

-- dsetDelElem {{{
dsetDelElem :: DSetNode -> String -> String -> IO ()
dsetDelElem node sname ename = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.delete ename
    _ -> return ()
-- }}}

-- dsetDelElemN {{{
dsetDelElemN :: DSetNode -> String -> [String] -> IO ()
dsetDelElemN node sname enames = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . flip (foldr Map.delete) enames
    _ -> return ()
-- }}}

-- dsetDelAll {{{
dsetDelAll :: DSetNode -> String -> IO ()
dsetDelAll node sname = do
  RWVar.modify_ (dsetMetaMap node) $ return . Map.delete sname
-- }}}

-- }}}

-- Storage info {{{

-- pullAllDataInfo {{{
pullAllDataInfo :: DSetNode -> IO [StorageInfo]
pullAllDataInfo node = do
  serverL <- listOnlineStorage $ confZKInfo $ dsetConfig node
  mapM pullDataInfo serverL
-- }}}

-- pullDataInfo {{{
pullDataInfo :: DServerInfo -> IO StorageInfo
pullDataInfo si = do
  mbfiles <- accessServer si (clientList False)
  mbcaches <- accessServer si (clientList True)
  let files = maybe Map.empty id mbfiles
  let caches = maybe Map.empty id mbcaches
  --return $ Map.union files caches
  return (si, files, caches)
-- }}}

-- updateStorageNode {{{
updateStorageNode :: DSetNode -> IO ()
updateStorageNode node = do
  storList <- pullAllDataInfo node
  let maxix = length storList - 1
  modifyMVar_ (dsetStorage node) $
    return . const (listArray (0, maxix) storList)
-- }}}

-- rollDice {{{
rollDice :: DSetNode -> IO Int
rollDice node = modifyMVar (dsetSeed node) $ \s -> return (s + 1, s)
-- }}}

-- pickStorageR {{{
pickStorageR :: DSetNode -> String -> IO (Maybe DServerInfo)
pickStorageR node ename = do
  arr <- readMVar (dsetStorage node)
  let (_, maxix) = bounds arr
  let m = maxix + 1
  r <- rollDice node
  lookupNStep m m r ename arr
  where
    lookupNStep 0 _ _ _ _ = return Nothing
    lookupNStep countdown m r ename arr = do
      let (si, fm, cm) = arr ! (r `mod` m)
      if Map.member ename fm || Map.member ename cm
      then return $ Just si
      else lookupNStep (countdown - 1) m (r + 1) ename arr
-- }}}

-- pickStorageW {{{
pickStorageW :: DSetNode -> IO (Maybe DServerInfo)
pickStorageW node = do
  arr <- readMVar (dsetStorage node)
  let (_, maxix) = bounds arr
  let m = maxix + 1
  r <- rollDice node
  if m == 0
  then return Nothing
  else do
    let (si, _, _) = arr ! (r `mod` m)
    return $ Just si
-- }}}

-- countDuplicate {{{
countDuplicate :: DSetNode -> String -> CheckSum -> IO Integer
countDuplicate node ename chksum = do
  storArr <- readMVar (dsetStorage node)
  return $ sum $ map getDup01 $ Arr.elems storArr
  where
    getDup01 (_, fm, cm) =
      if (Map.lookup ename fm == Just chksum) 
         || (Map.lookup ename cm == Just chksum)
      then 1
      else 0
-- }}}

-- }}}

-- server {{{

dsetServiceType :: String
dsetServiceType = "DSet"

-- loadMetaDataSet {{{
loadMetaDataSet :: DSetConfig -> IO DSetNode
loadMetaDataSet conf = do
  mbdataset <- loader `catch` aHandler Nothing
  datasetVar <- case mbdataset of
    Just dataset -> do
      rwvar <- Trav.mapM RWVar.new dataset
      RWVar.new rwvar
    _ -> do
      putStrLn "warning: DFS meta not loaded from file."
      RWVar.new Map.empty
  nodeMap <- newMVar $ listArray (0,0) []
  sequ <- newMVar 0
  return $ DSetNode datasetVar nodeMap sequ conf
  where
    loader = withFile (confMetaData conf) ReadMode getObject
-- }}}

-- makeDSetService {{{
makeDSetService :: ZKInfo -> FilePath -> IO DService
makeDSetService zkinfo rootpath = do
  conf <- makeConf zkinfo rootpath
  node <- loadMetaDataSet conf
  return $ DService dsetServiceType (dsetHandler node) Nothing
-- }}}

-- makeConf {{{
makeConf :: ZKInfo -> FilePath -> IO DSetConfig
makeConf zkinfo rootpath = do
  createDirectoryIfMissing True rootpath
  return $ DSetConfig (rootpath ++ "/dsetmeta") zkinfo
-- }}}

-- dsetRemoteLookup {{{
dsetRemoteLookup :: DSetNode -> String -> Handle -> IO ()
dsetRemoteLookup node sname remoteH = do
  respOK remoteH
  set <- dsetLookup node sname
  putObject remoteH set
  respOK remoteH
-- }}}

-- dsetRemotePickStorage {{{
dsetRemotePickStorageR :: DSetNode -> String -> Handle -> IO ()
dsetRemotePickStorageR node ename remoteH = do
  respOK remoteH
  mbstor <- pickStorageR node ename
  putObject remoteH mbstor
  respOK remoteH
-- }}}

-- dsetRemotePickStorageW {{{
dsetRemotePickStorageW :: DSetNode -> Handle -> IO ()
dsetRemotePickStorageW node remoteH = do
  respOK remoteH
  mbstor <- pickStorageW node
  putObject remoteH mbstor
  respOK remoteH
-- }}}

-- main handler {{{
dsetHandler :: DSetNode -> IOHandler
dsetHandler node remoteH = do
  mbReq <- getObject remoteH
  putStrLn $ "recv req: " ++ show mbReq
  case mbReq of
    Just req -> handleReq node remoteH req
    _ -> respFail remoteH
-- }}}

-- request handler {{{
handleReq :: DSetNode -> Handle -> DSetReq -> IO ()
handleReq node remoteH req = do
  case req of
    (DSetRAddElemN sname pairs) ->
      respOKDo $ dsetAddElemN node sname pairs
    (DSetRDelElemN sname enames) ->
      respOKDo $ dsetDelElemN node sname enames
    (DSetRDelAll sname) ->
      respOKDo $ dsetDelAll node sname
    (DSetRGetSet sname) ->
      respDo $ dsetRemoteLookup node sname remoteH
    (DSetRPickStorR ename) ->
      respDo $ dsetRemotePickStorageR node ename remoteH
    (DSetRPickStorW) ->
      respDo $ dsetRemotePickStorageW node remoteH
  where
    respDo op = op `catch` (ioaHandler (respFail remoteH) ())
    respOKDo op = respDo (op >> respOK remoteH)
-- }}}

-- }}}

-- client {{{
-- clientAddElemN {{{
clientAddElemN :: String -> [(String, CheckSum)] -> AHandler Bool
clientAddElemN sname values remoteH = do
  clientNoRecv (DSetRAddElemN sname values) remoteH
-- }}}
-- clientDelElemN {{{
clientDelElemN :: String -> [String] -> AHandler Bool
clientDelElemN sname enames remoteH = do
  clientNoRecv (DSetRDelElemN sname enames) remoteH
-- }}}
-- clientDelAll {{{
clientDelAll :: String -> AHandler Bool
clientDelAll sname remoteH = do
  clientNoRecv (DSetRDelAll sname) remoteH
-- }}}
-- clientGetSet {{{
clientGetSet :: String -> AHandler (Maybe DataSet)
clientGetSet sname remoteH = do
  clientRecvA (DSetRGetSet sname) remoteH
-- }}}
-- clientPickStorR {{{
clientPickStorR :: String -> AHandler (Maybe DServerInfo)
clientPickStorR ename remoteH = do
  join <$> clientRecvA (DSetRPickStorR ename) remoteH
-- }}}
-- clientPickStorW {{{
clientPickStorW :: AHandler (Maybe DServerInfo)
clientPickStorW remoteH = do
  join <$> clientRecvA (DSetRPickStorW) remoteH
-- }}}
-- }}}

-- DSet user {{{

-- lookup DSet server {{{
lookupDSetServer :: ZKInfo -> IO (Maybe DServerInfo)
lookupDSetServer zkinfo = do
  mbList <- listServer zkinfo
  case mbList of
    Just lst -> do
      case filter ((== dsetServiceType) . dsiServType) lst of
        (a:_) -> return $ Just a
        _ -> return Nothing
    _ -> return Nothing
-- }}}

-- uploadSingleton {{{
uploadSingleton :: ZKInfo -> String -> FilePath -> AHandler Bool
uploadSingleton zkinfo sname filepath remoteH = do
  mbDSet <- lookupDSetServer zkinfo
  case mbDSet of
    Just dset -> do
      mbstor <- join <$> accessServer dset clientPickStorW
      case mbstor of
        Just stor -> do
          let elemname = sname ++ "/" ++ sname
          chksum <- checkSumFile filepath
          metaOK <- accessServer dset (clientAddElemN sname [(elemname, chksum)])
          fileOK <- accessServer stor (clientPutFile False elemname filepath)
          return $ (fileOK == Just True) && (metaOK == Just True)
        _ -> return False
-- }}}

-- }}}

-- vim:fdm=marker
