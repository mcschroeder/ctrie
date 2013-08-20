{-# LANGUAGE BangPatterns #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
-----------------------------------------------------------------------
--
-- This is based on
--
--    * Aleksander Propoec, Phil Bagwell, Martin Odersky,
--      \"/Cache-Aware Lock-Free Concurent Hash Tries/\"
--
--    * Aleksander Prokopec, Nathan G. Bronson, Phil Bagwell,
--      Martin Odersky \"/Concurrent Tries with Efficient Non-Blocking
--      Snapshots/\"
--
-----------------------------------------------------------------------

-- [Note: CAS and pointer equality]
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- For the atomic compare-and-swap operation at the heart of this whole
-- thing, we need to compare two CNode objects. We can't really do an
-- actual '==' comparison, as that would entail a traversal of the whole
-- subtree. We could add a 'Unique' to the CNode and compare on that,
-- but we don't want the additional space overhead. That leaves us only
-- one option: pointer equality.
--
-- Pointer equality is a tricky subject in Haskell. There's the GHC
-- primitive 'reallyUnsafePtrEquality#' which as you can guess is
-- probably not such a good idea to use: it can return both false negatives
-- and false positives. (Although some people on the internet argue that
-- false positives can not actually occur, I don't want to take that chance.)
--
-- The alternative is 'StableName', which can have false negatives but not
-- false positives. And we don't actually care about false negatives, as then
-- the CAS will simply fail and the operation will be retried.
--
-- So 'StableName' seems like the winner, right? Well, there's just one
-- little snag: 'makeStableName' is in IO, so we can't use it inside
-- 'atomicModifyIORef' which takes a pure function. But you know what?
-- That's why we have 'unsafePerformIO'! *ducks*

module Control.Concurrent.Map where

import Control.Applicative ((<$>))
import Control.Monad
import Data.Bits
import Data.Hashable (Hashable)
import qualified Data.Hashable as H
import Data.IORef
import Data.Word
import System.IO.Unsafe  -- see [Note: CAS and pointer equality]
import System.Mem.StableName

-----------------------------------------------------------------------

-- TODO: list of branches in CNode should be an O(1) array of course

newtype Map    k v = Map (INode k v)
newtype INode  k v = INode (IORef (CNode k v))
data    CNode  k v = CNode !Bitmap ![Branch k v]
data    Branch k v = I !(INode k v)
                   | S !k v

type Bitmap = Word
type Hash   = Word

hash :: Hashable a => a -> Hash
hash = fromIntegral . H.hash

-----------------------------------------------------------------------

empty :: IO (Map k v)
empty = Map <$> INode <$> newIORef (CNode 0 [])

insert :: (Eq k, Hashable k) => k -> v -> Map k v -> IO ()
insert k0 v0 m@(Map i0) = do success <- go h0 k0 v0 0 i0
                             unless success $ insert k0 v0 m
    where
        h0 = hash k0
        go h k v lev (INode ref) = do
            cn@(CNode bmp arr) <- readIORef ref
            let (flag,pos) = flagpos bmp h lev
            if bmp .&. flag == 0
                then do
                    let arr' = arrayInsert (S k v) pos arr
                        cn'  = CNode (bmp .|. flag) arr'
                    compareAndSwap ref cn cn'
                else case arr !! pos of
                    I i2    -> go h k v (lev + w) i2
                    S k2 v2 -> if k == k2
                        then do
                            let arr' = arrayUpdate (S k v) pos arr
                                cn'  = CNode bmp arr'
                            compareAndSwap ref cn cn'
                        else do
                            let bmp2 = fromIntegral (lev + w)
                                arr2 = [(S k2 v2), (S k v)]
                            i2 <- INode <$> newIORef (CNode bmp2 arr2)
                            let arr' = arrayUpdate (I i2) pos arr
                                cn'  = CNode bmp arr'
                            compareAndSwap ref cn cn'

        w = 5

-----------------------------------------------------------------------

-- see [Note: CAS and pointer equality]
compareAndSwap :: IORef a -> a -> a -> IO Bool
compareAndSwap ref old new =
    atomicModifyIORef' ref (\cur -> if cur `ptrEq` old
                                    then (new, True)
                                    else (cur, False))
    where
        ptrEq !x !y = unsafePerformIO $ do
            sn1 <- makeStableName x
            sn2 <- makeStableName y
            return $ sn1 == sn2

-----------------------------------------------------------------------

flagpos :: Bitmap -> Hash -> Int -> (Word,Int)
flagpos bmp hc lev = (flag,pos)
    where
        flag = (hc `shiftR` (k * lev)) .&. ((1 `shiftL` k) - 1)
        pos = popCount ((flag - 1) .&. bmp)
        k = 5

-----------------------------------------------------------------------

-- TODO
arrayInsert :: a -> Int -> [a] -> [a]
arrayInsert x n xs = ys ++ [x] ++ zs
    where (ys,zs) = splitAt n xs

arrayUpdate :: a -> Int -> [a] -> [a]
arrayUpdate x n xs = ys ++ [x] ++ zs
    where
        (ys',zs) = splitAt n xs
        ys = if n == 0 then [] else tail ys'

-----------------------------------------------------------------------

-- TODO
printMap :: (Show k, Show v) => Map k v -> IO ()
printMap (Map i) = goI i
    where
        goI (INode ref) = putStr "(I " >> readIORef ref >>= goC >> putStr ")\n"
        goC (CNode bmp arr) = do
            putStr $ "(C " ++ (show bmp) ++ " ["
            mapM_ (\b -> goB b >> putStr ", ") arr
            putStr $ "] )"
        goB (I i) = putStr "\n" >> goI i
        goB (S k v) = putStr $ "(" ++ (show k) ++ "," ++ (show v) ++ ")"



