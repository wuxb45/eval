-- head {{{
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Types where
import Text.Show
--import Data.List
import Control.Applicative ((<$>), (<*>))
import qualified Data.Map as Map
import qualified Data.Sequence as Seq
import Control.Monad.Error
-- }}}

-- ErrorT {{{
data EvalError = EE String deriving Show
instance Error EvalError where
  noMsg = EE "EE ?!"
  strMsg = EE
type TError = Either EvalError
type IOTError = ErrorT EvalError IO

liftThrows :: TError a -> IOTError a
liftThrows (Left err) = throwError err
liftThrows (Right val) = return val
-- }}}

-- DataInfo {{{
type DataID = String
type Location = String
type DataSize = Integer

data DataInfo
  = DataInfo DataID DataSize Location
  deriving Show
-- }}}

-- StrMap a {{{
type StrMap a = Map.Map String a

type DataSet = StrMap DataInfo -- aka. "DS"
-- }}}

-- binding {{{
type Prim  = Env -> IOTError DataSet
type Pred  = Env -> IOTError Bool

data Binding
  = Expr  Expr
  | Data  DataSet
  | Prim  Prim
  | Pred  Pred

instance Show Binding where
  show (Expr expr) = show expr
  show (Data ds) = show ds
  show (Prim _) = "Prim"
  show (Pred _) = "Pred"

type BindingMap = StrMap Binding
-- }}}

-- PrimData* {{{
type PrimData1 = DataSet -> IOTError DataSet
type PrimData2 = DataSet -> PrimData1
type PrimData3 = DataSet -> PrimData2
type PrimDataN = [DataSet] -> IOTError DataSet

type PredData1 = DataSet -> IOTError Bool
type PredData2 = DataSet -> PredData1
type PredData3 = DataSet -> PredData2
type PredDataN = [DataSet] -> IOTError Bool

-- lookup DS {{{
lookupDS :: String -> Env -> TError DataSet
lookupDS p env = envGet p env >>= xDS
  where
    xDS :: Binding -> TError DataSet
    xDS b = case b of
      Data ds -> return ds
      _ -> throwError noMsg

lookupDSIO :: String -> Env -> IOTError DataSet
lookupDSIO p env = liftThrows $ lookupDS p env
-- }}}

--primDS* {{{
primDS1 :: String -> PrimData1 -> Prim
primDS1 p1 f = \env -> lookupDSIO p1 env >>= f

primDS2 :: String -> String -> PrimData2 -> Prim
primDS2 p1 p2 f = \env -> do
  ds1 <- lookupDSIO p1 env
  ds2 <- lookupDSIO p2 env
  f ds1 ds2

primDS3 :: String -> String -> String -> PrimData3 -> Prim
primDS3 p1 p2 p3 f = \env -> do
  ds1 <- lookupDSIO p1 env
  ds2 <- lookupDSIO p2 env
  ds3 <- lookupDSIO p3 env
  f ds1 ds2 ds3

primDSN :: [String] -> PrimDataN -> Prim
primDSN strList f = \env -> do
  dsList <- mapM (\p -> lookupDSIO p env) strList
  f dsList
-- }}}

--predDS* {{{
predDS1 :: String -> PredData1 -> Pred
predDS1 p1 f = \env -> lookupDSIO p1 env >>= f

predDS2 :: String -> String -> PredData2 -> Pred
predDS2 p1 p2 f = \env -> do
  ds1 <- lookupDSIO p1 env
  ds2 <- lookupDSIO p2 env
  f ds1 ds2

predDS3 :: String -> String -> String -> PredData3 -> Pred
predDS3 p1 p2 p3 f = \env -> do
  ds1 <- lookupDSIO p1 env
  ds2 <- lookupDSIO p2 env
  ds3 <- lookupDSIO p3 env
  f ds1 ds2 ds3

predDSN :: [String] -> PredDataN -> Pred
predDSN strList f = \env -> do
  dsList <- mapM (\p -> lookupDSIO p env) strList
  f dsList
-- }}}

-- }}}

-- Expr {{{
data Expr
  = Ref    String           -- reference to data
  | Lambda [String] Expr    -- lambda a b
  | Begin  [Expr]           -- do a b c ...
  | Par    [Expr]           -- parallel-do a b c ... collects all result
  | Apply   String [String] -- a = b (c,d,e ...)
  | If     Expr Expr Expr   -- if a then b else c
  | Let    [(String, Expr)] Expr -- let (a = b, c = d, ...) in c

-- Terminator.
--  Ref -> Ref (name of data-set)
--  Lambda -> Lambda (some Expr)

-- Non-terminator.
--  Begin -> return (result of last block)
--  Par -> return collection of [Expr]'s result
--  Apply -> return result
--  If -> Expr-True or Expr-False
--  Let -> add bindings to env, then return's the body

-- show Expr {{{
showD :: Int -> String
showD d = take d $ repeat ' '

showExpr :: Int -> Expr -> String
showExpr _ (Ref str) =
  "`" ++ str
showExpr d (Lambda _ expr) =
  "\\ * ->\n" ++ showD (d + 1) ++ (showExpr (d + 1) expr)
showExpr d (Begin exprL) =
  "-->\n" ++ concatMap (\e -> showD (d + 1)
                         ++ showExpr (d + 1) e ++ "\n") exprL
showExpr d (Par exprL) =
  "|->\n" ++ concatMap (\e -> showD (d + 1)
                         ++ showExpr (d + 1) e ++ "\n") exprL
showExpr d (Apply name params) =
  name ++ show params
showExpr d (If pred y n) =
  "IF (" ++ show pred ++ ")\n"
  ++ showD (d + 1) ++ showExpr (d + 1) y ++ "\n"
  ++ showD (d + 1) ++ showExpr (d + 1) n ++ "\n"
showExpr d (Let bindL body) =
  "Let\n"
  ++ concatMap (\(s,e) -> showD (d + 1) ++ show s
                 ++ " <- " ++ showExpr (d + 1) e ++ "\n") bindL
  ++ showD d ++ "IN\n"
  ++ showD (d + 1) ++ showExpr (d + 1) body

instance Show Expr where
  show = showExpr 2
-- }}}

-- }}}

-- Env {{{
type Env = BindingMap

envGet :: String -> Env -> TError Binding
envGet id bmap = maybe (throwError noMsg) return $ Map.lookup id bmap

envGetIO :: String -> Env -> IOTError Binding
envGetIO id map = liftThrows $ envGet id map

envPut :: String -> Binding -> Env -> Env
envPut id binding bmap = Map.insert id binding bmap

-- }}}

data Eval = Eval { evalEnv :: Env, evalExpr :: Expr }

-- vim:fdm=marker
