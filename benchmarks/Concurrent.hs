{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Applicative ((<$>))
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.DeepSeq
import Control.Exception (evaluate)
import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Criterion.Main
import Data.Hashable
import Data.List (foldl')
import Data.Foldable (foldlM)
import Data.Maybe
import System.Random
import System.Random.Shuffle
import System.IO.Unsafe
import Text.Printf

import qualified Control.Concurrent.Map as CM
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.HashMap.Strict as HM

instance (NFData k, NFData v) => NFData (CM.Map k v) where
    rnf m = rnf $ unsafePerformIO $ CM.unsafeToList m

main :: IO ()
main = do
    let o = 2^12
    let bench_ t o n io = env (mkElems t o n) $ \elems ->
                            bench (printf "t=%d, o=%d, n=%d" t o n) $ whnfIO $ io elems

    let bench_cm t o n = bench_ t o n $ \elems -> do
            m <- CM.empty
            runOps (\k -> CM.insert k k m) elems

        bench_hm_mvar t o n = bench_ t o n $ \elems -> do
            m <- newMVar HM.empty
            runOps (\k -> hm_insert k k m) elems

        bench_hm_tvar t o n = bench_ t o n $ \elems -> do
            m <- newTVarIO HM.empty
            runOps (\k -> hm_insert_tvar k k m) elems

    defaultMain
        [ bgroup "Insert"
            [ bgroup "Control.Concurrent.Map"
                [ bench_cm 100 o 1
                , bench_cm 100 o (o `div` 10)
                , bench_cm 100 o o
                , bench_cm 1000 o 1
                , bench_cm 1000 o (o `div` 10)
                , bench_cm 1000 o o
                ]
            , bgroup "Data.HashMap (TVar)"
                [ bench_hm_tvar 100 o 1
                , bench_hm_tvar 100 o (o `div` 10)
                , bench_hm_tvar 100 o o
                , bench_hm_tvar 1000 o 1
                , bench_hm_tvar 1000 o (o `div` 10)
                , bench_hm_tvar 1000 o o
                ]
            --, bgroup "Data.HashMap (MVar)"
            --    [ bench_hm_mvar 100 o 1
            --    , bench_hm_mvar 100 o (o `div` 10)
            --    , bench_hm_mvar 100 o o
            --    ]
            ]
        ]

mkElems :: Int -> Int -> Int -> IO [[Int]]
mkElems t o n = return $ [take o $ randomRs (0, n-1) (mkStdGen s) | s <- [1..t]]

runOps :: (k -> IO ()) -> [[k]] -> IO ()
runOps f elems = mapM_ wait =<< mapM (async . sequence_ . map f) elems

-----------------------------------------------------------------------
-- Control.Concurrent.Map

cm_lookup :: (Eq k, Hashable k) => k -> CM.Map k v -> IO ()
cm_lookup k m = do
    let v = CM.lookup k m
    v `seq` return ()
{-# SPECIALIZE cm_lookup :: String -> CM.Map String Int -> IO () #-}
{-# SPECIALIZE cm_lookup :: Int -> CM.Map Int Int -> IO () #-}

-----------------------------------------------------------------------
-- Data.HashMap (MVar)

hm_lookup :: (Eq k, Hashable k) => k -> MVar (HM.HashMap k v) -> IO ()
hm_lookup k mvar = do
    m <- takeMVar mvar
    putMVar mvar m
    let v = HM.lookup k m
    v `seq` return ()
{-# SPECIALIZE hm_lookup :: String -> MVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_lookup :: Int -> MVar (HM.HashMap Int Int) -> IO () #-}

hm_insert :: (Eq k, Hashable k) => k -> v -> MVar (HM.HashMap k v) -> IO ()
hm_insert k v mvar = do
    m <- takeMVar mvar
    putMVar mvar $! HM.insert k v m
{-# SPECIALIZE hm_insert :: String -> Int -> MVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_insert :: Int -> Int -> MVar (HM.HashMap Int Int) -> IO () #-}

hm_delete :: (Eq k, Hashable k) => k -> MVar (HM.HashMap k v) -> IO ()
hm_delete k mvar = do
    m <- takeMVar mvar
    putMVar mvar $! HM.delete k m
{-# SPECIALIZE hm_delete :: String -> MVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_delete :: Int -> MVar (HM.HashMap Int Int) -> IO () #-}

-----------------------------------------------------------------------
-- Data.HashMap (TVar)

hm_lookup_tvar :: (Eq k, Hashable k) => k -> TVar (HM.HashMap k v) -> IO ()
hm_lookup_tvar k tvar = do
    v <- atomically $ do m <- readTVar tvar
                         return $ HM.lookup k m
    v `seq` return ()
{-# SPECIALIZE hm_lookup_tvar :: String -> TVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_lookup_tvar :: Int -> TVar (HM.HashMap Int Int) -> IO () #-}

hm_insert_tvar :: (Eq k, Hashable k) => k -> v -> TVar (HM.HashMap k v) -> IO ()
hm_insert_tvar k v tvar = atomically $ modifyTVar' tvar (HM.insert k v)
{-# SPECIALIZE hm_insert_tvar :: String -> Int -> TVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_insert_tvar :: Int -> Int -> TVar (HM.HashMap Int Int) -> IO () #-}

hm_delete_tvar :: (Eq k, Hashable k) => k -> TVar (HM.HashMap k v) -> IO ()
hm_delete_tvar k tvar = atomically $ modifyTVar' tvar (HM.delete k)
{-# SPECIALIZE hm_delete_tvar :: String -> TVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_delete_tvar :: Int -> TVar (HM.HashMap Int Int) -> IO () #-}

