module Main where

import Control.Applicative ((<$>))
import Control.DeepSeq
import Control.Exception (evaluate)
import Control.Monad.IO.Class (liftIO)
import Criterion.Main
import Data.Hashable
import Data.List (foldl')
import Data.Foldable (foldlM)
import Data.Maybe
import System.Random
import System.IO.Unsafe

import qualified Control.Concurrent.Map as CM
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.HashMap.Strict as HM


-- like with the tests, a lot of this is cribbed from unordered-containers

-- TODO: make Ctrie a proper NFData instance
instance (NFData k, NFData v) => NFData (CM.Map k v) where
    rnf m = rnf $ unsafePerformIO $ CM.unsafeToList m

main :: IO ()
main = do
    let n = 2^12
        keysS = rndS 8 n
        keysI = rndI (n+n) n
        elemsS = zip keysS [1..n]
        elemsI = zip keysI [1..n]

    let mkConcMap = do
        cmS <- CM.fromList elemsS :: IO (CM.Map String Int)
        cmI <- CM.fromList elemsI :: IO (CM.Map Int Int)
        return (keysS, elemsS, cmS, keysI, elemsI, cmI)

    let mkMap = do
        let mS = M.fromList elemsS :: M.Map String Int
            mI = M.fromList elemsI :: M.Map Int Int
        return (keysS, elemsS, mS, keysI, elemsI, mI)

    let mkHashMap = do
        let hmS = HM.fromList elemsS :: HM.HashMap String Int
            hmI = HM.fromList elemsI :: HM.HashMap Int Int
        return (keysS, elemsS, hmS, keysI, elemsI, hmI)

    let mkIntMap = do
        let imI = IM.fromList elemsI :: IM.IntMap Int
        return (keysI, elemsI, imI)

    defaultMain [
          env mkConcMap $ \ ~(keysS, elemsS, cmS, keysI, elemsI, cmI) ->
          bgroup "Control.Concurrent.Map"
            [ bgroup "lookup"
                [ bench "String" $ whnfIO $ lookupCM keysS cmS
                , bench "Int" $ whnfIO $ lookupCM keysI $ cmI
                ]
            , bgroup "insert"
                [ bench "String" $ whnfIO $ insertCM elemsS =<< CM.empty
                , bench "Int" $ whnfIO $ insertCM elemsI =<< CM.empty
                ]
            , bgroup "delete"
                [ bench "String" $ whnfIO $ deleteCM keysS cmS
                , bench "Int" $ whnfIO $ deleteCM keysI cmI
                ]
            ]

        , env mkMap $ \ ~(keysS, elemsS, mS, keysI, elemsI, mI) ->
          bgroup "Data.Map"
            [ bgroup "lookup"
                [ bench "String" $ whnf (lookupM keysS) mS
                , bench "Int" $ whnf (lookupM keysI) mI
                ]
            , bgroup "insert"
                [ bench "String" $ whnf (insertM elemsS) M.empty
                , bench "Int" $ whnf (insertM elemsI) M.empty
                ]
            , bgroup "delete"
                [ bench "String" $ whnf (deleteM keysS) mS
                , bench "Int" $ whnf (deleteM keysI) mI
                ]
            ]

        , env mkHashMap $ \ ~(keysS, elemsS, hmS, keysI, elemsI, hmI) ->
          bgroup "Data.HashMap"
            [ bgroup "lookup"
                [ bench "String" $ whnf (lookupHM keysS) hmS
                , bench "Int" $ whnf (lookupHM keysI) hmI
                ]
            , bgroup "insert"
                [ bench "String" $ whnf (insertHM elemsS) HM.empty
                , bench "Int" $ whnf (insertHM elemsI) HM.empty
                ]
            , bgroup "delete"
                [ bench "String" $ whnf (deleteHM keysS) hmS
                , bench "Int" $ whnf (deleteHM keysI) hmI
                ]
            ]

        , env mkIntMap $ \ ~(keysI, elemsI, imI) ->
          bgroup "Data.IntMap"
            [ bgroup "lookup"
                [ bench "Int" $ whnf (lookupIM keysI) imI ]
            , bgroup "insert"
                [ bench "Int" $ whnf (insertIM elemsI) IM.empty ]
            , bgroup "delete"
                [ bench "Int" $ whnf (deleteIM keysI) imI ]
            ]
        ]



rndS :: Int -> Int -> [String]
rndS strlen num = take num $ split $ randomRs ('a', 'z') $ mkStdGen 1234
    where
        split cs = case splitAt strlen cs of (str, cs') -> str : split cs'

rndI :: Int -> Int -> [Int]
rndI upper num = take num $ randomRs (0, upper) $ mkStdGen 1234

-----------------------------------------------------------------------
-- Control.Concurrent.Map

lookupCM :: (Eq k, Hashable k) => [k] -> CM.Map k Int -> IO Int
lookupCM xs m = foldlM (\z k -> fromMaybe z <$> (CM.lookup k m)) 0 xs
{-# SPECIALIZE lookupCM :: [String] -> CM.Map String Int -> IO Int #-}
{-# SPECIALIZE lookupCM :: [Int] -> CM.Map Int Int -> IO Int #-}

insertCM :: (Eq k, Hashable k) => [(k, Int)] -> CM.Map k Int -> IO ()
insertCM xs m = mapM_ (\(k,v) -> CM.insert k v m) xs
{-# SPECIALIZE insertCM :: [(Int, Int)] -> CM.Map Int Int -> IO () #-}
{-# SPECIALIZE insertCM :: [(String, Int)] -> CM.Map String Int -> IO () #-}

deleteCM :: (Eq k, Hashable k) => [k] -> CM.Map k Int -> IO ()
deleteCM xs m = mapM_ (\k -> CM.delete k m) xs
{-# SPECIALIZE deleteCM :: [Int] -> CM.Map Int Int -> IO () #-}
{-# SPECIALIZE deleteCM :: [String] -> CM.Map String Int -> IO () #-}

-----------------------------------------------------------------------
-- Data.Map

lookupM :: Ord k => [k] -> M.Map k Int -> Int
lookupM xs m = foldl' (\z k -> fromMaybe z (M.lookup k m)) 0 xs
{-# SPECIALIZE lookupM :: [String] -> M.Map String Int -> Int #-}
{-# SPECIALIZE lookupM :: [Int] -> M.Map Int Int -> Int #-}

insertM :: Ord k => [(k, Int)] -> M.Map k Int -> M.Map k Int
insertM xs m0 = foldl' (\m (k, v) -> M.insert k v m) m0 xs
{-# SPECIALIZE insertM :: [(String, Int)] -> M.Map String Int -> M.Map String Int #-}
{-# SPECIALIZE insertM :: [(Int, Int)] -> M.Map Int Int -> M.Map Int Int #-}

deleteM :: Ord k => [k] -> M.Map k Int -> M.Map k Int
deleteM xs m0 = foldl' (\m k -> M.delete k m) m0 xs
{-# SPECIALIZE deleteM :: [String] -> M.Map String Int -> M.Map String Int #-}
{-# SPECIALIZE deleteM :: [Int] -> M.Map Int Int -> M.Map Int Int #-}

-----------------------------------------------------------------------
-- Data.HashMap

lookupHM :: (Eq k, Hashable k) => [k] -> HM.HashMap k Int -> Int
lookupHM xs m = foldl' (\z k -> fromMaybe z (HM.lookup k m)) 0 xs
{-# SPECIALIZE lookupHM :: [String] -> HM.HashMap String Int -> Int #-}
{-# SPECIALIZE lookupHM :: [Int] -> HM.HashMap Int Int -> Int #-}

insertHM :: (Eq k, Hashable k) => [(k, Int)] -> HM.HashMap k Int -> HM.HashMap k Int
insertHM xs m0 = foldl' (\m (k, v) -> HM.insert k v m) m0 xs
{-# SPECIALIZE insertHM :: [(Int, Int)] -> HM.HashMap Int Int -> HM.HashMap Int Int #-}
{-# SPECIALIZE insertHM :: [(String, Int)] -> HM.HashMap String Int -> HM.HashMap String Int #-}

deleteHM :: (Eq k, Hashable k) => [k] -> HM.HashMap k Int -> HM.HashMap k Int
deleteHM xs m0 = foldl' (\m k -> HM.delete k m) m0 xs
{-# SPECIALIZE deleteHM :: [Int] -> HM.HashMap Int Int -> HM.HashMap Int Int #-}
{-# SPECIALIZE deleteHM :: [String] -> HM.HashMap String Int -> HM.HashMap String Int #-}


-----------------------------------------------------------------------
-- Data.IntMap

lookupIM :: [Int] -> IM.IntMap Int -> Int
lookupIM xs m = foldl' (\z k -> fromMaybe z (IM.lookup k m)) 0 xs

insertIM :: [(Int, Int)] -> IM.IntMap Int -> IM.IntMap Int
insertIM xs m0 = foldl' (\m (k, v) -> IM.insert k v m) m0 xs

deleteIM :: [Int] -> IM.IntMap Int -> IM.IntMap Int
deleteIM xs m0 = foldl' (\m k -> IM.delete k m) m0 xs
