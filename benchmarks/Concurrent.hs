{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Applicative ((<$>))
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Exception (evaluate)
import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Criterion.Main
import Criterion.Config
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

main :: IO ()
main = do
    let hmS = HM.fromList elemsS :: HM.HashMap String Int
    hmS_mvar <- newMVar hmS
    let hmS_ops241_8     = hmOps hmS_mvar elemsS 8 n      (2,4,1)
    let hmS_ops621_n10_8 = hmOps hmS_mvar elemsS 8 (n*10) (6,2,1)

    cmS <- CM.fromList elemsS :: IO (CM.Map String Int)
    let cmS_ops241_8     = cmOps cmS elemsS 8 n      (2,4,1)
    let cmS_ops621_n10_8 = cmOps cmS elemsS 8 (n*10) (6,2,1)

    putStrLn $ "n=" ++ show n

    defaultMainWith defaultConfig
        (liftIO $ do
            evaluate $ rnf hmS
            evaluate $ sum $ map length hmS_ops241_8
            evaluate $ sum $ map length hmS_ops621_n10_8
            evaluate $ rnf $ unsafePerformIO $ CM.unsafeToList cmS
            evaluate $ sum $ map length cmS_ops241_8
            evaluate $ sum $ map length cmS_ops621_n10_8
            return ()
        )
        [
            bgroup "Data.HashMap (MVar)"
            [ bgroup "String"
                [ bench "8 threads, n, 2:4:1"   $ runAll hmS_ops241_8
                , bench "8 threads, 10n, 6:2:1" $ runAll hmS_ops621_n10_8
                ]
            ]
            ,
            bgroup "Control.Concurrent.Map"
            [ bgroup "String"
                [ bench "8 threads, n, 2:4:1"   $ runAll cmS_ops241_8
                , bench "8 threads, 10n, 6:2:1" $ runAll cmS_ops621_n10_8
                ]
            ]
        ]
    where
        n = 2^12
        elemsS = zip keysS [1..n]
        elemsI = zip keysI [1..n]
        keysS = rndS 8 n
        keysI = rndI (n+n) n


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

        ops = take nThreads
            $ iterate (\ops -> shuffle' ops nOps (mkStdGen 1234))
            $ take nOps ops0
    in ops

-----------------------------------------------------------------------
-- Control.Concurrent.Map

cmOps = mkOps cm_lookup CM.insert CM.delete

cm_lookup :: (Eq k, Hashable k) => k -> CM.Map k v -> IO ()
cm_lookup k m = do
    let v = CM.lookup k m
    v `seq` return ()
{-# SPECIALIZE cm_lookup :: String -> CM.Map String Int -> IO () #-}
{-# SPECIALIZE cm_lookup :: Int -> CM.Map Int Int -> IO () #-}


-----------------------------------------------------------------------
-- Data.HashMap

hmOps = mkOps hm_lookup hm_insert hm_delete

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


