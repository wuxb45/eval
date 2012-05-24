module Eval.Lib where
import qualified Data.Map as Map
import Eval.Types

define :: String -> Expr -> Expr -> Expr
define name expr (Let bindings body) = Let ((name, expr):bindings) body
define name expr expr1 = Let [(name, expr)] expr1

parmap :: Expr
parmap = Lambda ["mapper", "res-list"] $
  Let [("first", Apply "first-res" ["res-list"]),
       ("rest", Apply "rest-res" ["res-list"])]
      (Par [Apply "mapper" ["first"],
            If (Apply "empty?" ["rest"])
               (Begin [])
               (Apply "parmap" ["rest"])])

reduce :: Expr
reduce = Lambda ["combiner", "res-list"] $
  Let [("first",  Apply "first-res"  ["res-list"]),
       ("second", Apply "second-res" ["res-list"]),
       ("rest-2", Apply "rest-2-res" ["res-list"])]
      (Let [("tmp", Apply "combiner" ["first", "second"])]
           (If (Apply "empty?" ["rest-2"])
               (Ref "tmp")
               (Let [("new-res-list", Apply "cons" ["tmp", "rest-2"])]
                    (Apply "reduce" ["conbiner", "new-res-list"]))))

simpleMR :: Expr
simpleMR = define "parmap" parmap $ define "reduce" reduce $
  Let [("input-ds", (Apply "getInput" ["input-list"])),
       ("interm-ds", (Apply "parmap" ["prim-wc-wc", "input-list"]))]
      (Apply "reduce" ["prim-wc-collect", "interm-ds"])

primBindings :: Map.Map String Binding
primBindings = Map.fromList $
  [("first-res", Prim firstPrim),
   ("second-res", Prim secondPrim),
   ("rest-2", Prim rest2Prim),
   ("rest-res", Prim rest2Prim),
   ("empty?", Pred emptyPred) ]
      
-- prim {{{

-- union
unionDS :: PrimData2
unionDS ds1 ds2 = return $ Map.union ds1 ds2

unionPrim :: Prim
unionPrim = primDS2 "$1" "$2" unionDS

-- first
firstDS :: PrimData1
firstDS ds = case Map.toList ds of
  ((k,v):_) -> return $ Map.singleton k v
  _ -> return Map.empty

firstPrim :: Prim
firstPrim = primDS1 "$1" firstDS

-- rest
restDS :: PrimData1
restDS ds = case Map.toList ds of
  (_:rest) -> return $ Map.fromList rest
  _ -> return Map.empty

restPrim :: Prim
restPrim = primDS1 "$1" firstDS

-- second
secondDS :: PrimData1
secondDS ds = case Map.toList ds of
  (_:(k,v):_) -> return $ Map.singleton k v
  _ -> return Map.empty

secondPrim :: Prim
secondPrim = primDS1 "$1" secondDS

-- rest2
rest2DS :: PrimData1
rest2DS ds = case Map.toList ds of
  (_:_:rest2) -> return $ Map.fromList rest2
  _ -> return Map.empty

rest2Prim :: Prim
rest2Prim = primDS1 "$1" rest2DS

-- empty?
emptyDS :: PredData1
emptyDS ds = return $ Map.null ds

emptyPred :: Pred
emptyPred = predDS1 "$1" emptyDS

-- }}}

-- vim:fdm=marker
