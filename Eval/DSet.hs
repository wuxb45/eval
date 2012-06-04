-- Copyright 2012 Wu Xingbo

-- head {{{
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- }}}

-- module export {{{
module Eval.DSet (
  -- data
  DSetConfig(..), DataSet, DSetMetaMap,
  MetaCountMap, MetaFullMap, MetaSimpleMap,
  StorageInfo, DSetNode(..), DSetReq(..),
  -- client
  clientAddElemN, clientDelElemN, clientDelAll,
  clientGetSet, clientPickStorR, clientPickStorW,
  clientSimpleMap, clientFullMap, clientCountMap,
  clientBackup, clientDSetCmdH, clientCmdH,
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
import System.Directory (createDirectoryIfMissing)
import GHC.Generics (Generic)
import Text.Printf (printf)
------
import Eval.DServ (ZKInfo(..), DServerInfo(..), DService(..),
                   AHandler, IOHandler, ioaHandler,
                   accessServer, forkServer, waitCloseSignal, listServer,
                   forkChildrenWatcher,
                   commonInitial, aHandler, clientRecvA, clientNoRecv,
                   respOK, respFail, putObject, getObject,)
import Eval.Storage (CheckSum, DSDirInfo, listOnlineStorage, clientList,
                     clientPutBS, checkSumBS, openBinBufFile, clientGetH,)
               
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
-- DSetMetaMap {{{
type DSetMetaMap = Map String (RWVar.RWVar DataSet)
-- }}}
-- Meta*Map {{{
type MetaSimpleMap = Map String [String]
type MetaFullMap = Map String [(String, CheckSum)]
type MetaCountMap = Map String [(String, CheckSum, Integer)]
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
  = DSetRAddElemN    String [(String, CheckSum)]
  | DSetRAddElem     String (String, CheckSum)
  | DSetRDelElemN    String [String]
  | DSetRDelElem     String String
  | DSetRDelAll      String
  | DSetRGetSet      String -- returns DataSet
  | DSetRPickStorR   String -- returns (Maybe DServerInfo)
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
  withFile (confMetaData $ dsetConfig node) WriteMode (flip putObject pureMap)
-- }}}

-- dsetLookup {{{
dsetLookup :: DSetNode -> String -> IO DataSet
dsetLookup node sname = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  maybe (return Map.empty) (\s -> RWVar.with s return) mbset
-- }}}

-- dsetAddElem {{{
dsetAddElem :: DSetNode -> String -> (String, CheckSum) -> IO ()
dsetAddElem node sname (k,v) = do
  let newMap = Map.singleton k v
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.union newMap
    _ -> do
      set <- RWVar.new newMap
      RWVar.modify_ (dsetMetaMap node) $ return . Map.insert sname set
  --dsetBackup node
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
  --dsetBackup node
-- }}}

-- dsetDelElem {{{
dsetDelElem :: DSetNode -> String -> String -> IO ()
dsetDelElem node sname ename = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.delete ename
    _ -> return ()
  --dsetBackup node
-- }}}

-- dsetDelElemN {{{
dsetDelElemN :: DSetNode -> String -> [String] -> IO ()
dsetDelElemN node sname enames = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . flip (foldr Map.delete) enames
    _ -> return ()
  --dsetBackup node
-- }}}

-- dsetDelAll {{{
dsetDelAll :: DSetNode -> String -> IO ()
dsetDelAll node sname = do
  RWVar.modify_ (dsetMetaMap node) $ return . Map.delete sname
  --dsetBackup node
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
pickStorageR :: DSetNode -> String -> IO (Maybe DServerInfo)
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
dsetRemoteLookup :: DSetNode -> String -> Handle -> IO ()
dsetRemoteLookup node sname remoteH = do
  respOK remoteH
  set <- dsetLookup node sname
  putObject remoteH set
  respOK remoteH
-- }}}

-- dsetRemotePickStorageR {{{
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
-- clientSimpleMap {{{
clientSimpleMap :: AHandler (Maybe MetaSimpleMap)
clientSimpleMap remoteH = do
  clientRecvA DSetRSimpleMap remoteH

clientSimpleMapList :: AHandler [String]
clientSimpleMapList remoteH = do
  tree <- maybe Map.empty id <$> clientSimpleMap remoteH
  return $ concat $ Map.elems $
    Map.mapWithKey (\k v -> ((k ++ " ## ") ++) <$> v) tree
-- }}}
-- clientFullMap {{{
clientFullMap :: AHandler (Maybe MetaFullMap)
clientFullMap remoteH = do
  clientRecvA DSetRFullMap remoteH

clientFullMapList :: AHandler [String]
clientFullMapList remoteH = do
  tree <- maybe Map.empty id <$> clientFullMap remoteH
  return $ concat $ Map.elems $
    Map.mapWithKey (\k v -> ((k ++ " ## ") ++) . show <$> v) tree
-- }}}
-- clientCountMap {{{
clientCountMap :: AHandler (Maybe MetaCountMap)
clientCountMap remoteH = do
  clientRecvA DSetRCountMap remoteH

clientCountMapList :: AHandler [String]
clientCountMapList remoteH = do
  tree <- maybe Map.empty id <$> clientCountMap remoteH
  return $ concat $ Map.elems $
    Map.mapWithKey (\k v -> ((k ++ " ## ") ++) . show <$> v) tree
-- }}}
-- clientBackup {{{
clientBackup :: AHandler Bool
clientBackup remoteH = do
  clientNoRecv (DSetRBackup) remoteH
-- }}}
-- clientDSetCmd {{{
clientDSetCmdH :: [String] -> AHandler (Either Bool [String])
clientDSetCmdH = clientCmdH

clientCmdH :: [String] -> AHandler (Either Bool [String])
clientCmdH ("addm1":sname:ename:len:chksum:[]) =
  (Left <$>) . clientAddElemN sname [(ename, (read len, chksum))]
clientCmdH ("delm1":sname:ename:[]) =
  (Left <$>) . clientDelElemN sname [ename]
clientCmdH ("delm":sname:[]) =
  (Left <$>) . clientDelAll sname
clientCmdH ("getm":sname:[]) =
  (Right . map show . maybe [] Map.toList <$>) . clientGetSet sname
clientCmdH ("pickr":ename:[]) =
  (Right . return . show <$>) . clientPickStorR ename
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
clientCmdH _ = return . const (Right ["unknown command!"])

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
putDSetBS :: DServerInfo -> String -> String -> BS.ByteString -> IO Bool
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
makeStdEname :: String -> Integer -> String
makeStdEname sname ix = printf "%s/%s/%09d" sname sname ix
-- }}}

-- putSingleton {{{
putSingleton :: DServerInfo -> String -> FilePath -> IO Bool
putSingleton dset sname filepath = do
  let ename = makeStdEname sname 0
  bs <- BS.readFile filepath
  putDSetBS dset sname ename bs
-- }}}

-- putSplit {{{
dsetSplitLimit :: Int
dsetSplitLimit = 0x8000000 -- 128M

putSplit :: DServerInfo -> String -> FilePath -> IO Bool
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
getFile :: DServerInfo -> String -> FilePath -> IO Bool
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
        ok <- putSingleton server sname filename
        putStrLn $ "upload single: " ++ show ok
      ("put":sname:filename:[]) -> do
        ok <- putSplit server sname filename
        putStrLn $ "upload split: " ++ show ok
      ("get":sname:filename:[]) -> do
        ok <- getFile server sname filename
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
