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
--
-- TODO:
-- unordered-containers uses 'reallyUnsafePtrEquality' so we might as well

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
import Text.Printf

-----------------------------------------------------------------------

-- TODO: list of branches in CNode should be an O(1) array of course
-- TODO: deal with hash collisions

newtype Map    k v = Map (INode k v)
newtype INode  k v = INode (IORef (CNode k v))
data    CNode  k v = CNode !Bitmap ![Branch k v]
data    Branch k v = I !(INode k v)
                   | S !k v

type Bitmap = Word
type Hash   = Word
type Level  = Int

hash :: Hashable a => a -> Hash
hash = fromIntegral . H.hash

-----------------------------------------------------------------------

empty :: IO (Map k v)
empty = Map <$> INode <$> newIORef (CNode 0 [])


insert :: (Eq k, Hashable k) => k -> v -> Map k v -> IO ()
insert k v m@(Map inode) =
    let h = hash k in repeatUntilTrue (iinsert h k v 0 inode)

iinsert :: (Eq k, Hashable k) => Hash -> k -> v -> Level -> INode k v -> IO Bool
iinsert h k v lev (INode ref) = do
    cn@(CNode bmp arr) <- readIORef ref
    let m = mask h lev
        i = sparseIndex bmp m
    if bmp .&. m == 0
        then let arr' = arrayInsert (S k v) i arr
                 cn'  = CNode (bmp .|. m) arr'
             in compareAndSwap ref cn cn'
        else case arr !! i of
            I inode2 -> iinsert h k v (nextLevel lev) inode2
            S k2 v2
                | k == k2 -> do
                    let arr' = arrayUpdate (S k v) i arr
                        cn'  = CNode bmp arr'
                    compareAndSwap ref cn cn'
                | otherwise -> do
                    inode2 <- newINode h (S k v) (hash k2) (S k2 v2) (nextLevel lev)
                    let arr' = arrayUpdate (I inode2) i arr
                        cn'  = CNode bmp arr'
                    compareAndSwap ref cn cn'

newINode :: Hash -> Branch k v -> Hash -> Branch k v -> Int -> IO (INode k v)
newINode h1 b1 h2 b2 lev = do
    let i1 = index h1 lev
        i2 = index h2 lev
        bmp = (unsafeShiftL 1 i1) .|. (unsafeShiftL 1 i2)
    ref <- case compare i1 i2 of
        LT -> newIORef $ CNode bmp [b1,b2]
        GT -> newIORef $ CNode bmp [b2,b1]
        EQ -> do inode' <- newINode h1 b1 h2 b2 (nextLevel lev)
                 newIORef $ CNode bmp [I inode']
    return (INode ref)


repeatUntilTrue :: Monad m => m Bool -> m ()
repeatUntilTrue act = do
    success <- act
    if success
        then return ()
        else repeatUntilTrue act
{-# INLINE repeatUntilTrue #-}

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

bitsPerSubkey :: Int
bitsPerSubkey = floor . logBase 2 . fromIntegral . bitSize $ (undefined :: Word)

subkeyMask :: Bitmap
subkeyMask = 1 `unsafeShiftL` bitsPerSubkey - 1

index :: Hash -> Level -> Int
index h lev = fromIntegral $ (h `unsafeShiftR` lev) .&. subkeyMask
{-# INLINE index #-}

-- when or-ed with a CNode bitmap, determines if the hash is present
-- in the array at the given level of the trie
mask :: Hash -> Level -> Bitmap
mask h lev = 1 `unsafeShiftL` index h lev
{-# INLINE mask #-}

-- position in the CNode array
sparseIndex :: Bitmap -> Bitmap -> Int
sparseIndex bmp m = popCount ((m - 1) .&. bmp)
{-# INLINE sparseIndex #-}

nextLevel :: Level -> Level
nextLevel = (+) bitsPerSubkey
{-# INLINE nextLevel #-}


-----------------------------------------------------------------------

-- TODO
arrayInsert :: a -> Int -> [a] -> [a]
arrayInsert x n xs = ys ++ [x] ++ zs
    where (ys,zs) = splitAt n xs

arrayUpdate :: a -> Int -> [a] -> [a]
arrayUpdate x n xs = ys ++ [x] ++ zs
    where (ys,_:zs) = splitAt n xs


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



