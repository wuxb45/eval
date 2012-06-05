-- Copyright 2012 Wu Xingbo

-- head {{{
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
-- }}}

-- module export {{{
module Eval.DSet (
  -- data
  DSetConfig(..), DataSet, DSetMetaMap,
  MetaCountMap, MetaFullMap, MetaSimpleMap,
  StorageInfo, DSetNode(..), DSetReq(..),
  -- client
  clientAddElem, clientDelElem,
  clientAddElemN, clientDelElemN, clientDelAll,
  clientGetSet, clientPickStorR, clientPickStorW,
  clientSimpleMap, clientFullMap, clientCountMap,
  clientBackup, clientCmdH,
  -- user
  lookupDSetServer, putDSetBS, makeStdEname,
  runDSetClientREPL, putSingleton, putSplit, getFile,
  runDSetSimpleServer,
  ) where
-- }}}

-- import {{{
import qualified Data.Map as Map
import qualified Data.Array as Arr
import qualified Data.Traversable as Trav
import qualified Data.ByteString as BS
import qualified Control.Concurrent.ReadWriteVar as RWVar
------
import Prelude (($), (.), (+), (>), (==), (++), (-), (/=), (||), (&&),
                flip, Bool(..), compare, fromIntegral, Int,
                IO, String, Show(..), Integer, id, toInteger,
                const, read, snd, mod, undefined,)
import Control.Applicative ((<$>), )
import Control.Monad (Monad(..), mapM, mapM_, join, void,)
import Control.Concurrent (MVar, readMVar, modifyMVar_, newMVar,
                           modifyMVar, threadDelay, forkIO,)
import Control.Exception (catch,)
import Data.Array.IArray
import Data.Either (Either(..))
import Data.Map (Map)
import Data.Maybe (Maybe(..), maybe, )
import Data.Serialize (Serialize(..),)
import System.IO (Handle, withFile, IOMode(..),
                  getLine, putStrLn, hFlush, stdout,)
import Data.List (map, filter, foldr, length,
                  concat, sum, words,)
import System.FilePath (FilePath, )
import System.Directory (createDirectoryIfMissing, renameFile,)
import GHC.Generics (Generic)
import Text.Printf (printf)
------
import Eval.DServ (ZKInfo(..), DServerInfo(..), DService(..),
                   AHandler, IOHandler, ioaHandler,
                   accessServer, forkServer, waitCloseSignal, listServer,
                   forkChildrenWatcher,
                   commonInitial, aHandler, clientRecvA, clientNoRecv,
                   respOK, respFail, putObject, getObject, )
import Eval.Storage (CheckSum, DSDirInfo, listOnlineStorage, clientList,
                     clientPutBS, checkSumBS, openBinBufFile, clientGetH,
                     uniqueName, toKey, fromKey, showCheckSum, Key,
                     readSHA1,)
               
-- }}}

-- data {{{
-- DFSConfig {{{
data DSetConfig = DSetConfig
  { confMetaData :: FilePath,
    confZKInfo   :: ZKInfo }
-- }}}
-- DataSet {{{
type DataSet = Map Key CheckSum
-- }}}
-- DSetMetaMap {{{
type DSetMetaMap = Map Key (RWVar.RWVar DataSet)
-- }}}
-- Meta*Map {{{
type MetaSimpleMap = Map Key [Key]
type MetaFullMap = Map Key [(Key, CheckSum)]
type MetaCountMap = Map Key [(Key, CheckSum, Integer)]
-- }}}
-- DSetNode {{{
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
  = DSetRAddElemN    Key [(Key, CheckSum)]
  | DSetRAddElem     Key (Key, CheckSum)
  | DSetRDelElemN    Key [Key]
  | DSetRDelElem     Key Key
  | DSetRDelAll      Key
  | DSetRGetSet      Key -- returns DataSet
  | DSetRPickStorR   Key -- returns (Maybe DServerInfo)
  | DSetRPickStorW
  | DSetRSimpleMap
  | DSetRFullMap
  | DSetRCountMap
  | DSetRBackup
  deriving (Show, Generic)
instance Serialize DSetReq where
-- }}}
-- }}}

-- DSet {{{

-- dsetBackup {{{
dsetBackup :: DSetNode -> IO ()
dsetBackup node = do
  dsMap <- RWVar.with (dsetMetaMap node) return
  pureMap <- Trav.mapM (flip RWVar.with return) dsMap
  let meta = confMetaData $ dsetConfig node
  suffix <- uniqueName
  let tmpmeta = meta ++ ".tmp." ++ suffix
  withFile tmpmeta WriteMode (flip putObject pureMap)
  renameFile tmpmeta meta
-- }}}

-- dsetLookup {{{
dsetLookup :: DSetNode -> Key -> IO DataSet
dsetLookup node sname = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  maybe (return Map.empty) (\s -> RWVar.with s return) mbset
-- }}}

-- dsetAddElem {{{
dsetAddElem :: DSetNode -> Key -> (Key, CheckSum) -> IO ()
dsetAddElem node sname (k,v) = do
  let newMap = Map.singleton k v
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.union newMap
    _ -> do
      set <- RWVar.new newMap
      RWVar.modify_ (dsetMetaMap node) $ return . Map.insert sname set
-- }}}

-- dsetAddElemN {{{
dsetAddElemN :: DSetNode -> Key -> [(Key, CheckSum)] -> IO ()
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
dsetDelElem :: DSetNode -> Key -> Key -> IO ()
dsetDelElem node sname ename = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.delete ename
    _ -> return ()
-- }}}

-- dsetDelElemN {{{
dsetDelElemN :: DSetNode -> Key -> [Key] -> IO ()
dsetDelElemN node sname enames = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . flip (foldr Map.delete) enames
    _ -> return ()
-- }}}

-- dsetDelAll {{{
dsetDelAll :: DSetNode -> Key -> IO ()
dsetDelAll node sname = do
  RWVar.modify_ (dsetMetaMap node) $ return . Map.delete sname
-- }}}

-- dsetSimpleMap {{{
dsetSimpleMap :: DSetNode -> IO MetaSimpleMap
dsetSimpleMap node = do
  tree <- RWVar.with (dsetMetaMap node) return
  Trav.mapM (flip RWVar.with (return . Map.keys)) tree
-- }}}

-- dsetFullMap {{{
dsetFullMap :: DSetNode -> IO MetaFullMap
dsetFullMap node = do
  tree <- RWVar.with (dsetMetaMap node) return
  Trav.mapM (flip RWVar.with (return . Map.toList)) tree
-- }}}

-- dsetCountMap {{{
dsetCountMap :: DSetNode -> IO MetaCountMap
dsetCountMap node = do
  fullMap <- dsetFullMap node
  countMap <- Trav.mapM (mapM addCount) fullMap
  return countMap
  where
    addCount (name, chksum) = do
      count <- countDuplicate node name chksum
      return (name, chksum, count)
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
  --putStrLn $ "update current storage: " ++ (show $ length storList)
  let maxix = length storList - 1
  modifyMVar_ (dsetStorage node) $
    return . const (listArray (0, maxix) storList)
-- }}}

-- rollDice {{{
rollDice :: DSetNode -> IO Int
rollDice node = modifyMVar (dsetSeed node) $ \s -> return (s + 1, s)
-- }}}

-- pickStorageR {{{
pickStorageR :: DSetNode -> Key -> IO (Maybe DServerInfo)
pickStorageR node ename = do
  arr <- readMVar (dsetStorage node)
  let (_, maxix) = bounds arr
  let m = maxix + 1
  r <- rollDice node
  lookupNStep m m r arr
  where
    lookupNStep 0 _ _ _ = return Nothing
    lookupNStep countdown m r arr = do
      let (si, fm, cm) = arr ! (r `mod` m)
      if Map.member ename fm || Map.member ename cm
      then return $ Just si
      else lookupNStep (countdown - 1) m (r + 1) arr
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
countDuplicate :: DSetNode -> Key -> CheckSum -> IO Integer
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
  mbdsMap <- loader `catch` aHandler Nothing
  dsMapMVar <- case mbdsMap of
    Just dsMap -> do
      rwvar <- Trav.mapM RWVar.new dsMap
      RWVar.new rwvar
    _ -> do
      putStrLn "warning: DFS meta not loaded from file."
      RWVar.new Map.empty
  nodeMap <- newMVar $ listArray (0, (-1)) []
  sequ <- newMVar 0
  return $ DSetNode dsMapMVar nodeMap sequ conf
  where
    loader = withFile (confMetaData conf) ReadMode getObject
-- }}}

-- makeDSetService {{{
makeDSetService :: ZKInfo -> FilePath -> IO DService
makeDSetService zkinfo rootpath = do
  conf <- makeConf zkinfo rootpath
  node <- loadMetaDataSet conf
  startStorageMonitor node zkinfo
  return $ DService dsetServiceType (dsetHandler node) Nothing
-- }}}

-- startStorageMonitor {{{
startStorageMonitor :: DSetNode -> ZKInfo -> IO ()
startStorageMonitor node zkinfo = do
  -- update on event
  void $ forkChildrenWatcher zkinfo "/d" $ const updater
  -- every 10 sec. 1,000,000 => 1 sec
  void $ forkIO repeatUpdater
  where
    updater = updateStorageNode node
    repeatUpdater = do
      dsetBackup node
      updater
      threadDelay 10000000
      repeatUpdater
-- }}}

-- makeConf {{{
makeConf :: ZKInfo -> FilePath -> IO DSetConfig
makeConf zkinfo rootpath = do
  createDirectoryIfMissing True rootpath
  return $ DSetConfig (rootpath ++ "/dsetmeta") zkinfo
-- }}}

-- dsetRemoteLookup {{{
dsetRemoteLookup :: DSetNode -> Key -> Handle -> IO ()
dsetRemoteLookup node sname remoteH = do
  respOK remoteH
  set <- dsetLookup node sname
  putObject remoteH set
  respOK remoteH
-- }}}

-- dsetRemotePickStorageR {{{
dsetRemotePickStorageR :: DSetNode -> Key -> Handle -> IO ()
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

-- dsetRemoteSimpleMap {{{
dsetRemoteSimpleMap :: DSetNode -> Handle -> IO ()
dsetRemoteSimpleMap node remoteH = do
  respOK remoteH
  tree <- dsetSimpleMap node
  --putStrLn $ "send simple tree back" ++ show tree
  putObject remoteH tree
  respOK remoteH
-- }}}

-- dsetRemoteFullMap {{{
dsetRemoteFullMap :: DSetNode -> Handle -> IO ()
dsetRemoteFullMap node remoteH = do
  respOK remoteH
  tree <- dsetFullMap node
  --putStrLn $ "send full tree back" ++ show tree
  putObject remoteH tree
  respOK remoteH
-- }}}

-- dsetRemoteCountMap {{{
dsetRemoteCountMap :: DSetNode -> Handle -> IO ()
dsetRemoteCountMap node remoteH = do
  respOK remoteH
  tree <- dsetCountMap node
  --putStrLn $ "send count tree back" ++ show tree
  putObject remoteH tree
  respOK remoteH
-- }}}

-- main handler {{{
dsetHandler :: DSetNode -> IOHandler
dsetHandler node remoteH = do
  mbReq <- getObject remoteH
  --putStrLn $ "recv req: " ++ show mbReq
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
    (DSetRAddElem sname pair) ->
      respOKDo $ dsetAddElem node sname pair
    (DSetRDelElemN sname enames) ->
      respOKDo $ dsetDelElemN node sname enames
    (DSetRDelElem sname ename) ->
      respOKDo $ dsetDelElem node sname ename
    (DSetRDelAll sname) ->
      respOKDo $ dsetDelAll node sname
    (DSetRGetSet sname) ->
      respDo $ dsetRemoteLookup node sname remoteH
    (DSetRPickStorR ename) ->
      respDo $ dsetRemotePickStorageR node ename remoteH
    (DSetRPickStorW) ->
      respDo $ dsetRemotePickStorageW node remoteH
    (DSetRSimpleMap) ->
      respDo $ dsetRemoteSimpleMap node remoteH
    (DSetRFullMap) ->
      respDo $ dsetRemoteFullMap node remoteH
    (DSetRCountMap) ->
      respDo $ dsetRemoteCountMap node remoteH
    (DSetRBackup) ->
      respOKDo $ dsetBackup node
  where
    respDo op = op `catch` (ioaHandler (respFail remoteH) ())
    respOKDo op = respDo (op >> respOK remoteH)
-- }}}

-- }}}

-- client {{{
-- clientAddElem {{{
clientAddElem :: Key -> (Key, CheckSum) -> AHandler Bool
clientAddElem sname value remoteH = do
  clientNoRecv (DSetRAddElem sname value) remoteH
-- }}}
-- clientDelElem {{{
clientDelElem :: Key -> Key -> AHandler Bool
clientDelElem sname ename remoteH = do
  clientNoRecv (DSetRDelElem sname ename) remoteH
-- }}}
-- clientAddElemN {{{
clientAddElemN :: Key -> [(Key, CheckSum)] -> AHandler Bool
clientAddElemN sname values remoteH = do
  clientNoRecv (DSetRAddElemN sname values) remoteH
-- }}}
-- clientDelElemN {{{
clientDelElemN :: Key -> [Key] -> AHandler Bool
clientDelElemN sname enames remoteH = do
  clientNoRecv (DSetRDelElemN sname enames) remoteH
-- }}}
-- clientDelAll {{{
clientDelAll :: Key -> AHandler Bool
clientDelAll sname remoteH = do
  clientNoRecv (DSetRDelAll sname) remoteH
-- }}}
-- clientGetSet {{{
clientGetSet :: Key -> AHandler (Maybe DataSet)
clientGetSet sname remoteH = do
  clientRecvA (DSetRGetSet sname) remoteH
-- }}}
-- clientPickStorR {{{
clientPickStorR :: Key -> AHandler (Maybe DServerInfo)
clientPickStorR ename remoteH = do
  join <$> clientRecvA (DSetRPickStorR ename) remoteH
-- }}}
-- clientPickStorW {{{
clientPickStorW :: AHandler (Maybe DServerInfo)
clientPickStorW remoteH = do
  join <$> clientRecvA (DSetRPickStorW) remoteH
-- }}}
-- clientSimpleMap {{{
clientSimpleMap :: AHandler (Maybe MetaSimpleMap)
clientSimpleMap remoteH = do
  clientRecvA DSetRSimpleMap remoteH

clientSimpleMapList :: AHandler [String]
clientSimpleMapList remoteH = do
  tree <- maybe Map.empty id <$> clientSimpleMap remoteH
  return $ concat $ Map.elems $
    Map.mapWithKey (\k v -> ((fromKey k ++ "## ") ++) . fromKey <$> v) tree
-- }}}
-- clientFullMap {{{
clientFullMap :: AHandler (Maybe MetaFullMap)
clientFullMap remoteH = do
  clientRecvA DSetRFullMap remoteH

clientFullMapList :: AHandler [String]
clientFullMapList remoteH = do
  tree <- maybe Map.empty id <$> clientFullMap remoteH
  return $ concat $ Map.elems $
    Map.mapWithKey (\k v -> ((fromKey k ++ "## ") ++) . showPair <$> v) tree
  where
    showPair (k, cs) = fromKey k ++ ", " ++ showCheckSum cs
-- }}}
-- clientCountMap {{{
clientCountMap :: AHandler (Maybe MetaCountMap)
clientCountMap remoteH = do
  clientRecvA DSetRCountMap remoteH

clientCountMapList :: AHandler [String]
clientCountMapList remoteH = do
  tree <- maybe Map.empty id <$> clientCountMap remoteH
  return $ concat $ Map.elems $
    Map.mapWithKey (\k v -> ((fromKey k ++ " ## ") ++) . showPair <$> v) tree
  where
    showPair (k, cs, n) = fromKey k ++ ", " ++ showCheckSum cs ++ ", " ++ show n
-- }}}
-- clientBackup {{{
clientBackup :: AHandler Bool
clientBackup remoteH = do
  clientNoRecv (DSetRBackup) remoteH
-- }}}
-- clientDSetCmd {{{
clientCmdH :: [String] -> AHandler (Either Bool [String])
clientCmdH ("addm1":sname:ename:len:chksum:[]) =
  (Left <$>) . clientAddElem (toKey sname) ((toKey ename), (read len, readSHA1 chksum))
clientCmdH ("delm1":sname:ename:[]) =
  (Left <$>) . clientDelElem (toKey sname) (toKey ename)
clientCmdH ("delm":sname:[]) =
  (Left <$>) . clientDelAll (toKey sname)
clientCmdH ("getm":sname:[]) =
  (Right . map show . maybe [] Map.toList <$>) . clientGetSet (toKey sname)
clientCmdH ("pickr":ename:[]) =
  (Right . return . show <$>) . clientPickStorR (toKey ename)
clientCmdH ("pickw":[]) =
  (Right . return . show <$>) . clientPickStorW
clientCmdH ("ls":[]) =
  (Right <$>) . clientSimpleMapList
clientCmdH ("lf":[]) =
  (Right <$>) . clientFullMapList
clientCmdH ("lc":[]) =
  (Right <$>) . clientCountMapList
clientCmdH ("backup":[]) =
  (Left <$>) . clientBackup
clientCmdH _ = return . const (Right ["?? type \'h\' for help"])
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

-- putDSetBS {{{
putDSetBS :: DServerInfo -> Key -> Key -> BS.ByteString -> IO Bool
putDSetBS dset sname ename bs = do
  mbstor <- join <$> accessServer dset clientPickStorW
  case mbstor of
    Just stor -> do
      let chksum = checkSumBS bs
      metaOK <- accessServer dset (clientAddElemN sname [(ename, chksum)])
      fileOK <- accessServer stor (clientPutBS False ename bs)
      return $ (fileOK == Just True) && (metaOK == Just True)
    _ -> return False
-- }}}

-- makeStdEname {{{
makeStdEname :: Key -> Integer -> Key
makeStdEname sname ix = toKey $ printf "%s/%s/%09d" str str ix
  where str = fromKey sname
-- }}}

-- putSingleton {{{
putSingleton :: DServerInfo -> Key -> FilePath -> IO Bool
putSingleton dset sname filepath = do
  let ename = makeStdEname sname 0
  bs <- BS.readFile filepath
  putDSetBS dset sname ename bs
-- }}}

-- putSplit {{{
dsetSplitLimit :: Int
dsetSplitLimit = 0x8000000 -- 128M

putSplit :: DServerInfo -> Key -> FilePath -> IO Bool
putSplit dset sname filepath = do
  h <- openBinBufFile filepath ReadMode
  recPut h 0
  where
    recPut h (ix :: Integer) = do
      bs <- BS.hGet h dsetSplitLimit
      if BS.length bs /= 0
      then do
        let ename = makeStdEname sname ix
        ok <- putDSetBS dset sname ename bs
        if ok
        then recPut h (ix + 1)
        else return False
      else return False
-- }}}

-- getFile {{{
getFile :: DServerInfo -> Key -> FilePath -> IO Bool
getFile dset sname filepath = do
  mbdataset <- join <$> accessServer dset (clientGetSet sname)
  case mbdataset of
    Just dataset -> do
      h <- openBinBufFile filepath WriteMode
      recGet dataset h 0
    _ -> return False
  where
    recGet dataset h (ix :: Integer) = do
      let ename = makeStdEname sname ix
      if Map.member ename dataset
      then do
        mbStor <- join <$> accessServer dset (clientPickStorR ename)
        case mbStor of
          Just stor -> do
            segOK <- maybe False id <$> accessServer stor (clientGetH False ename h)
            if segOK
            then recGet dataset h (ix + 1)
            else return False
          _ -> return False
      else return True
-- }}}

-- runDSetSimpleServer {{{
runDSetSimpleServer :: ZKInfo -> Integer -> FilePath -> IO ()
runDSetSimpleServer zkinfo port rootpath = do
  commonInitial
  service <- makeDSetService zkinfo rootpath
  mbsd <- forkServer zkinfo service port
  maybe (return ()) waitCloseSignal mbsd
-- }}}

-- printHelp {{{
printHelp :: IO ()
printHelp = mapM_ putStrLn $
  [ "> put1   <sname> <filename>"
  , "> put    <sname> <filename>"
  , "> get    <sname> <filename>"
  , "> addm1   <sname> <ename> <length> <checksum>"
  , "> delm1   <sname> <ename>"
  , "> delm    <sname>"
  , "> getm    <sname>"
  , "> pickr  <ename>"
  , "> pickw"
  , "> ls"
  , "> lf"
  , "> lc"
  , "> backup"]
-- }}}

-- runClientREPL {{{
runDSetClientREPL :: ZKInfo -> IO ()
runDSetClientREPL zkinfo = do
  commonInitial
  mbserver <- lookupDSetServer zkinfo
  case mbserver of
    Just server -> runLoopREPL zkinfo server
    _ -> putStrLn "no DSet server"

runLoopREPL :: ZKInfo -> DServerInfo -> IO ()
runLoopREPL zkinfo server = do
  putStrLn $ "==============================================================="
  putStrLn $ "==============================================================="
  printf "DSet > " >> hFlush stdout
  cmd <- words <$> (getLine `catch` aHandler "quit")
  if cmd == ("quit":[])
  then putStrLn "bye!" >> hFlush stdout >> return ()
  else do
    case cmd of
      ("h":[]) -> printHelp
      ("put1":sname:filename:[]) -> do
        ok <- putSingleton server (toKey sname) filename
        putStrLn $ "upload single: " ++ show ok
      ("put":sname:filename:[]) -> do
        ok <- putSplit server (toKey sname) filename
        putStrLn $ "upload split: " ++ show ok
      ("get":sname:filename:[]) -> do
        ok <- getFile server (toKey sname) filename
        putStrLn $ "get file: " ++ show ok
      _ -> accessAndPrint server cmd
    runLoopREPL zkinfo server
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

-- vim:fdm=marker
