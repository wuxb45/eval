-- Copyright 2012 Wu Xingbo
-- head {{{
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
-- }}}

-- module export {{{
module Eval.DSet where
-- }}}

-- import {{{
import qualified Data.Map as Map
import qualified Data.Array as Arr
import qualified Data.Traversable as Trav
import qualified Control.Concurrent.ReadWriteVar as RWVar
------
import Prelude (($), (.), (+), (>), (==), (++), (-), (/=), (||), (&&),
                flip, Bool(..), compare, fromIntegral, Int,
                IO, String, Show(..), Integer, id, toInteger,
                const, read, snd, mod,)
import Control.Applicative ((<$>), )
import Control.Monad (Monad(..), mapM, mapM_, join, sequence_, void,)
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
import Data.List (map, filter, foldr, length, sum, words, repeat,)
import System.FilePath (FilePath, )
import System.Directory (createDirectoryIfMissing)
import GHC.Generics (Generic)
import Text.Printf (printf)
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
  dsetBackup node
-- }}}

-- dsetDelElem {{{
dsetDelElem :: DSetNode -> String -> String -> IO ()
dsetDelElem node sname ename = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . Map.delete ename
    _ -> return ()
  dsetBackup node
-- }}}

-- dsetDelElemN {{{
dsetDelElemN :: DSetNode -> String -> [String] -> IO ()
dsetDelElemN node sname enames = do
  mbset <- RWVar.with (dsetMetaMap node) $ return . Map.lookup sname
  case mbset of
    Just set -> do
      RWVar.modify_ set $ return . flip (foldr Map.delete) enames
    _ -> return ()
  dsetBackup node
-- }}}

-- dsetDelAll {{{
dsetDelAll :: DSetNode -> String -> IO ()
dsetDelAll node sname = do
  RWVar.modify_ (dsetMetaMap node) $ return . Map.delete sname
  dsetBackup node
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
  putStrLn $ "update current storage: " ++ (show $ length storList)
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
  void $ forkIO $ sequence_ $ repeat $ updater >> threadDelay 10000000
  where
    updater = updateStorageNode node
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
-- clientDSetCmd {{{
clientDSetCmdH :: [String] -> AHandler (Either Bool [String])
clientDSetCmdH = clientCmdH

clientCmdH :: [String] -> AHandler (Either Bool [String])
clientCmdH ("add1":sname:ename:len:chksum:[]) =
  (Left <$>) . clientAddElemN sname [(ename, (read len, chksum))]
clientCmdH ("del1":sname:ename:[]) =
  (Left <$>) . clientDelElemN sname [ename]
clientCmdH ("del":sname:[]) =
  (Left <$>) . clientDelAll sname
clientCmdH ("get":sname:[]) =
  (Right . map show . maybe [] Map.toList <$>) . clientGetSet sname
clientCmdH ("pickr":ename:[]) =
  (Right . return . show <$>) . clientPickStorR ename
clientCmdH ("pickw":[]) =
  (Right . return . show <$>) .clientPickStorW
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

-- uploadSingleton {{{
uploadSingleton :: ZKInfo -> String -> FilePath -> IO Bool
uploadSingleton zkinfo sname filepath = do
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
    _ -> return False
-- }}}

-- runSimpleServer {{{
runDSetSimpleServer :: ZKInfo -> Integer -> FilePath -> IO ()
runDSetSimpleServer zkinfo port rootpath = do
  commonInitial
  service <- makeDSetService zkinfo rootpath
  mbsd <- forkServer zkinfo service port
  maybe (return ()) waitCloseSignal mbsd
-- }}}

-- runClientREPL {{{
runDSetClientREPL :: ZKInfo -> IO ()
runDSetClientREPL zkinfo = do
  commonInitial
  mbserver <- lookupDSetServer zkinfo
  case mbserver of
    Just server -> runLoopREPL server
    _ -> putStrLn "no DSet server"

runLoopREPL :: DServerInfo -> IO ()
runLoopREPL server = do
  putStrLn $ "==============================================================="
  putStrLn $ "==============================================================="
  printf "DSet > " >> hFlush stdout
  cmd <- words <$> (getLine `catch` aHandler "quit")
  if cmd == ["quit"]
  then putStrLn "bye!" >> hFlush stdout >> return ()
  else do
    accessAndPrint server cmd
    runLoopREPL server
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
