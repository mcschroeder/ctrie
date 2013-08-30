{-# LANGUAGE BangPatterns #-}

-- | Convenient interface for 'Data.Primitive.Array'.
module Control.Concurrent.Map.Array
    ( Array
    , empty, singleton, pair
    , head, index
    , insert, update, delete
    , mapM, mapM_, foldM'
    ) where

import Control.Monad.Primitive
import Control.Monad.ST
import Data.Primitive.Array
import Prelude hiding (head, mapM, mapM_)

-----------------------------------------------------------------------

empty :: Array a
empty = runST $ unsafeFreezeArray =<< newArray 0 undefined
{-# INLINE empty #-}

singleton :: a -> Array a
singleton x = runST $ do
    marr <- newArray 1 x
    unsafeFreezeArray marr
{-# INLINE singleton #-}

pair :: a -> a -> Array a
pair x y = runST $ do
    marr <- newArray 2 undefined
    writeArray marr 0 x
    writeArray marr 1 y
    unsafeFreezeArray marr
{-# INLINE pair #-}

-----------------------------------------------------------------------

head :: Array a -> a
head = flip indexArray 0
{-# INLINE head #-}

index :: Array a -> Int -> a
index = indexArray
{-# INLINE index #-}

-----------------------------------------------------------------------

insert :: a -> Int -> Int -> Array a -> Array a
insert x i n arr = runST $ do
    marr <- newArray (n+1) undefined
    copyArray marr 0 arr 0 i
    writeArray marr i x
    copyArray marr (i+1) arr i (n-i)
    unsafeFreezeArray marr
{-# INLINE insert #-}

update :: a -> Int -> Int -> Array a -> Array a
update x i n arr = runST $ do
    marr <- newArray n undefined
    copyArray marr 0 arr 0 n
    writeArray marr i x
    unsafeFreezeArray marr
{-# INLINE update #-}

delete :: Int -> Int -> Array a -> Array a
delete i n arr = runST $ do
    marr <- newArray (n-1) undefined
    copyArray marr 0 arr 0 i
    copyArray marr i arr (i+1) (n-(i+1))
    unsafeFreezeArray marr
{-# INLINE delete #-}

-----------------------------------------------------------------------

mapM :: PrimMonad m => (a -> m b) -> Int -> Array a -> m (Array b)
mapM f n arr = do
    marr <- newArray n undefined
    go arr marr 0 n
    unsafeFreezeArray marr
    where
        go arr marr i n
            | i >= n = return ()
            | otherwise = do
                x <- indexArrayM arr i
                writeArray marr i =<< f x
                go arr marr (i+1) n
{-# INLINE mapM #-}

mapM_ :: PrimMonad m => (a -> m b) -> Int -> Array a -> m ()
mapM_ f n arr = go arr 0 n
    where
        go arr i n
            | i >= n = return ()
            | otherwise = do
                x <- indexArrayM arr i
                _ <- f x
                go arr (i+1) n
{-# INLINE mapM_ #-}

foldM' :: PrimMonad m => (b -> a -> m b) -> b -> Int -> Array a -> m b
foldM' f z0 n arr0 = go arr0 n 0 z0
    where
        go arr n i !z
            | i >= n = return z
            | otherwise = do
                x <- indexArrayM arr i
                go arr n (i+1) =<< f z x
{-# INLINE foldM' #-}
