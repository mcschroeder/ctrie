{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# LANGUAGE BangPatterns #-}

module Data.SparseArray
    ( SparseArray
    , Hashable, Hash, hash
    , Level, down, up, lastLevel
    , emptyArray, mkSingleton, mkPair
    , arrayLookup, arrayInsert, arrayUpdate, arrayDelete
    , arrayMapM, arrayMapM_, arrayFoldM'
    , arrayToMaybe
    ) where

import Control.Monad.ST
import Data.Bits
import Data.Hashable (Hashable)
import qualified Data.Hashable as H
import Data.Primitive.Array
import Data.Word
import Prelude hiding (lookup, mapM, mapM_)

-----------------------------------------------------------------------

data SparseArray a = SparseArray !Bitmap !(Array a)

type Bitmap = Word
type Hash   = Word
type Level  = Int

-----------------------------------------------------------------------

emptyArray :: SparseArray a
emptyArray = SparseArray 0 arr
  where
    arr = runST $ do
        marr <- newArray 0 undefined
        unsafeFreezeArray marr

{-# INLINE emptyArray #-}

mkSingleton :: Level -> Hash -> a -> SparseArray a
mkSingleton level h a = SparseArray bmp arr
  where
    i   = index level h
    bmp = unsafeShiftL 1 i
    arr = runST $ do
        marr <- newArray 1 a
        unsafeFreezeArray marr

{-# INLINE mkSingleton #-}

mkPair :: Level -> Hash -> a -> Hash -> a -> Maybe (SparseArray a)
mkPair level h1 a1 h2 a2 =
    case compare i1 i2 of
        LT -> Just $ SparseArray bmp (pair a1 a2)
        GT -> Just $ SparseArray bmp (pair a2 a1)
        EQ -> Nothing
  where
    i1  = index level h1
    i2  = index level h2
    bmp = (unsafeShiftL 1 i1) .|. (unsafeShiftL 1 i2)
    pair x y = runST $ do
        marr <- newArray 2 undefined
        writeArray marr 0 x
        writeArray marr 1 y
        unsafeFreezeArray marr

{-# INLINE mkPair #-}

arrayLookup :: Level -> Hash -> SparseArray a -> Maybe a
arrayLookup level h (SparseArray bmp arr)
    | bmp .&. m == 0 = Nothing
    | otherwise      = Just (indexArray arr i)
  where
    m = mask level h
    i = sparseIndex bmp m

{-# INLINE arrayLookup #-}

arrayInsert :: Level -> Hash -> a -> SparseArray a -> SparseArray a
arrayInsert level h a (SparseArray bmp arr) = SparseArray bmp' arr'
  where
    n    = popCount bmp
    m    = mask level h
    i    = sparseIndex bmp m
    bmp' = bmp .|. m
    arr' = runST $ do
        marr <- newArray (n+1) undefined
        copyArray marr 0 arr 0 i
        writeArray marr i a
        copyArray marr (i+1) arr i (n-i)
        unsafeFreezeArray marr

{-# INLINE arrayInsert #-}

arrayUpdate :: Level -> Hash -> a -> SparseArray a -> SparseArray a
arrayUpdate level h a (SparseArray bmp arr) = SparseArray bmp arr'
  where
    n = popCount bmp
    m = mask level h
    i = sparseIndex bmp m
    arr' = runST $ do
        marr <- newArray n undefined
        copyArray marr 0 arr 0 n
        writeArray marr i a
        unsafeFreezeArray marr

{-# INLINE arrayUpdate #-}

arrayDelete :: Level -> Hash -> SparseArray a -> SparseArray a
arrayDelete level h (SparseArray bmp arr) = SparseArray bmp' arr'
  where
    n    = popCount bmp
    m    = mask level h
    i    = sparseIndex bmp m
    bmp' = bmp `xor` m
    arr' = runST $ do
        marr <- newArray (n-1) undefined
        copyArray marr 0 arr 0 i
        copyArray marr i arr (i+1) (n-(i+1))
        unsafeFreezeArray marr

{-# INLINE arrayDelete #-}

arrayMapM :: (a -> IO a) -> SparseArray a -> IO (SparseArray a)
arrayMapM f = \(SparseArray bmp arr) -> do
    let n = popCount bmp
    marr <- newArray n undefined
    go n arr marr 0
    arr' <- unsafeFreezeArray marr
    return (SparseArray bmp arr')
  where
    go n arr marr i
        | i >= n = return ()
        | otherwise = do
            x <- indexArrayM arr i
            writeArray marr i =<< f x
            go n arr marr (i+1)

{-# INLINE arrayMapM #-}

arrayMapM_ :: (a -> IO b) -> SparseArray a -> IO ()
arrayMapM_ f = \(SparseArray bmp arr) -> do
    let n = popCount bmp
    go n arr 0
  where
    go n arr i
        | i >= n = return ()
        | otherwise = do
            x <- indexArrayM arr i
            _ <- f x
            go n arr (i+1)

{-# INLINE arrayMapM_ #-}

arrayFoldM' :: (b -> a -> IO b) -> b -> SparseArray a -> IO b
arrayFoldM' f z0 = \(SparseArray bmp arr) -> do
    let n = popCount bmp
    go n arr 0 z0
  where
    go n arr i !z
        | i >= n = return z
        | otherwise = do
            x <- indexArrayM arr i
            go n arr (i+1) =<< f z x

{-# INLINE arrayFoldM' #-}

arrayToMaybe :: SparseArray a -> Maybe a
arrayToMaybe (SparseArray bmp arr) =
    case popCount bmp of
        1 -> Just $ indexArray arr 0
        _ -> Nothing

{-# INLINE arrayToMaybe #-}

-----------------------------------------------------------------------

hash :: Hashable a => a -> Hash
hash = fromIntegral . H.hash
{-# INLINE hash #-}

hashLength :: Int
hashLength = finiteBitSize (undefined :: Word)
{-# INLINE hashLength #-}

bitsPerSubkey :: Int
bitsPerSubkey = floor . logBase (2 :: Float) . fromIntegral $ hashLength
{-# INLINE bitsPerSubkey #-}

subkeyMask :: Bitmap
subkeyMask = 1 `unsafeShiftL` bitsPerSubkey - 1
{-# INLINE subkeyMask #-}

down :: Level -> Level
down = (+) bitsPerSubkey
{-# INLINE down #-}

up :: Level -> Level
up = subtract bitsPerSubkey
{-# INLINE up #-}

lastLevel :: Level
lastLevel = hashLength
{-# INLINE lastLevel #-}

index :: Level -> Hash -> Int
index level h = fromIntegral $ (h `unsafeShiftR` level) .&. subkeyMask
{-# INLINE index #-}

-- when or-ed with a bitmap, determines if the hash is present
-- in the array at the given level of the trie
mask :: Level -> Hash -> Bitmap
mask level h = 1 `unsafeShiftL` index level h
{-# INLINE mask #-}

-- position in the array
sparseIndex :: Bitmap -> Bitmap -> Int
sparseIndex bmp m = popCount ((m - 1) .&. bmp)
{-# INLINE sparseIndex #-}
