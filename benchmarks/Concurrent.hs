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

import qualified Control.Concurrent.Map as CM
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.HashMap.Strict as HM


instance (NFData k, NFData v) => NFData (CM.Map k v) where
    rnf m = rnf $ unsafePerformIO $ CM.unsafeToList m

main :: IO ()
main = do
    let n = 2^12
        elemsS = zip keysS [1..n]
        elemsI = zip keysI [1..n]
        keysS = rndS 8 n
        keysI = rndI (n+n) n

    putStrLn $ "n=" ++ show n

    let mkConcMap = do
        cmS <- CM.fromList elemsS :: IO (CM.Map String Int)
        return (elemsS, cmS)

    let mkHashMap = do
        let hmS = HM.fromList elemsS :: HM.HashMap String Int
        return (elemsS, hmS)

    defaultMain
        [ env mkConcMap $ \ ~(elemsS, cmS) ->
          bgroup "Control.Concurrent.Map"
            [ bench "8 threads, n, 2:4:1"   $ whnfIO $ runCM cmS elemsS 8  n     (2,4,1)
            , bench "8 threads, 10n, 6:2:1" $ whnfIO $ runCM cmS elemsS 8 (n*10) (6,2,1)
            ]

        , env mkHashMap $ \ ~(elemsS, hmS) ->
          bgroup "Data.HashMap (MVar)"
            [ bench "8 threads, n, 2:4:1"   $ whnfIO $ runHM_mvar hmS elemsS 8  n     (2,4,1)
            , bench "8 threads, 10n, 6:2:1" $ whnfIO $ runHM_mvar hmS elemsS 8 (n*10) (6,2,1)
            ]

        , env mkHashMap $ \ ~(elemsS, hmS) ->
          bgroup "Data.HashMap (TVar)"
            [ bench "8 threads, n, 2:4:1"   $ whnfIO $ runHM_tvar hmS elemsS 8  n     (2,4,1)
            , bench "8 threads, 10n, 6:2:1" $ whnfIO $ runHM_tvar hmS elemsS 8 (n*10) (6,2,1)
            ]
        ]


rndS :: Int -> Int -> [String]
rndS strlen num = take num $ split $ randomRs ('a', 'z') $ mkStdGen 1234
    where
        split cs = case splitAt strlen cs of (str, cs') -> str : split cs'

rndI :: Int -> Int -> [Int]
rndI upper num = take num $ randomRs (0, upper) $ mkStdGen 1234

runAll :: [[IO ()]] -> IO ()
runAll ops = do
    as <- mapM (async . sequence_) ops
    mapM_ wait as

mkOps :: (k -> m -> IO ())       -- lookup function
      -> (k -> v -> m -> IO ())  -- insert function
      -> (k -> m -> IO ())       -- delete function
      -> m -> [(k,v)]            -- the map & the elements from which to draw
      -> Int                     -- the number of threads
      -> Int                     -- the number of operations per thread
      -> (Int, Int, Int)         -- ratio of lookups : insertions : deletions
      -> [[IO ()]]
mkOps lookup insert delete m elems nThreads nOps (rl,ri,rd) =
    let tot = fromIntegral $ rl + ri + rd
        numLookups = ceiling $ fromIntegral nOps * (fromIntegral rl / tot)
        numInserts = ceiling $ fromIntegral nOps * (fromIntegral ri / tot)
        numDeletes = ceiling $ fromIntegral nOps * (fromIntegral rd / tot)

        lookupElems = shuffle' (take nOps $ cycle elems) nOps (mkStdGen 1234)
        insertElems = shuffle' (take nOps $ cycle elems) nOps (mkStdGen 5678)
        deleteElems = shuffle' (take nOps $ cycle elems) nOps (mkStdGen 9012)

        ops0 = [lookup k   m | (k,_) <- take numLookups lookupElems]
            ++ [insert k v m | (k,v) <- take numInserts insertElems]
            ++ [delete k   m | (k,_) <- take numDeletes deleteElems]

        !ops = take nThreads
            $! iterate (\ops -> shuffle' ops nOps (mkStdGen 1234))
            $! take nOps ops0
    in ops

-----------------------------------------------------------------------
-- Control.Concurrent.Map

runCM cm elems nThreads nOps ratios = do
    runAll $ mkOps cm_lookup CM.insert CM.delete cm elems nThreads nOps ratios

cm_lookup :: (Eq k, Hashable k) => k -> CM.Map k v -> IO ()
cm_lookup k m = do
    let v = CM.lookup k m
    v `seq` return ()
{-# SPECIALIZE cm_lookup :: String -> CM.Map String Int -> IO () #-}
{-# SPECIALIZE cm_lookup :: Int -> CM.Map Int Int -> IO () #-}


-----------------------------------------------------------------------
-- Data.HashMap (MVar)

runHM_mvar hm elems nThreads nOps ratios = do
    hm_mvar <- newMVar hm
    runAll $ mkOps hm_lookup hm_insert hm_delete hm_mvar elems nThreads nOps ratios

hm_lookup :: (Eq k, Hashable k) => k -> MVar (HM.HashMap k v) -> IO ()
hm_lookup k mvar = do
    --return undefined
    m <- takeMVar mvar
    putMVar mvar m
    let v = HM.lookup k m
    v `seq` return ()
{-# SPECIALIZE hm_lookup :: String -> MVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_lookup :: Int -> MVar (HM.HashMap Int Int) -> IO () #-}

hm_insert :: (Eq k, Hashable k) => k -> v -> MVar (HM.HashMap k v) -> IO ()
hm_insert k v mvar = do
    --return ()
    m <- takeMVar mvar
    putMVar mvar $! HM.insert k v m
{-# SPECIALIZE hm_insert :: String -> Int -> MVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_insert :: Int -> Int -> MVar (HM.HashMap Int Int) -> IO () #-}

hm_delete :: (Eq k, Hashable k) => k -> MVar (HM.HashMap k v) -> IO ()
hm_delete k mvar = do
    --return ()
    m <- takeMVar mvar
    putMVar mvar $! HM.delete k m
{-# SPECIALIZE hm_delete :: String -> MVar (HM.HashMap String Int) -> IO () #-}
{-# SPECIALIZE hm_delete :: Int -> MVar (HM.HashMap Int Int) -> IO () #-}

-----------------------------------------------------------------------
-- Data.HashMap (TVar)

runHM_tvar hm elems nThreads nOps ratios = do
    hm_tvar <- newTVarIO hm
    runAll $ mkOps hm_lookup_tvar hm_insert_tvar hm_delete_tvar hm_tvar elems nThreads nOps ratios

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

