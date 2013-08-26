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

module Control.Concurrent.Map
    ( Map
    , empty
    , insert
    , remove
    , lookup
    , fromList
    , unsafeToList
    ) where

import Control.Applicative ((<$>))
import Control.Monad
import Data.Bits
import Data.Hashable (Hashable)
import qualified Data.Hashable as H
import qualified Data.List as List
import Data.IORef
import Data.Word
import System.IO.Unsafe  -- see [Note: CAS and pointer equality]
import System.Mem.StableName
import Text.Printf
import Prelude hiding (lookup)

-----------------------------------------------------------------------

-- TODO: list of branches in CNode should be an O(1) array of course
-- TODO: deal with hash collisions

newtype Map   k v = Map (INode k v)
newtype INode k v = INode (IORef (MainNode k v))
data MainNode k v = CNode !Bitmap ![Branch k v]
                  | Tomb !k v
data Branch   k v = I !(INode k v)
                  | S !k v

isTomb :: MainNode k v -> Bool
isTomb (Tomb _ _) = True
isTomb _          = False

type Bitmap = Word
type Hash   = Word
type Level  = Int

hash :: Hashable a => a -> Hash
hash = fromIntegral . H.hash

-----------------------------------------------------------------------
-- * Construction

empty :: IO (Map k v)
empty = Map <$> INode <$> newIORef (CNode 0 [])


-----------------------------------------------------------------------
-- * Modification

insert :: (Eq k, Hashable k) => k -> v -> Map k v -> IO ()
insert k v (Map root) = go 0 undefined root
    where
        h = hash k
        go lev parent inode@(INode ref) = do
            main <- readIORef ref
            case main of
                cn@(CNode bmp arr) -> do
                    let m = mask h lev
                        i = sparseIndex bmp m
                    if bmp .&. m == 0
                        then insertTip ref cn i m
                        else case arr !! i of
                            I inode2 -> go (nextLevel lev) inode inode2
                            S k2 v2 | k == k2 -> updateTip ref cn i
                                    | otherwise -> extend ref cn i k2 v2 lev
                Tomb _ _ -> do
                    clean parent (prevLevel lev)
                    go 0 undefined root

        insertTip ref cn@(CNode bmp arr) i m = do
            let arr' = arrayInsert (S k v) i arr
                cn'  = CNode (bmp .|. m) arr'
            ok <- compareAndSwap ref cn cn'
            unless ok $ go 0 undefined root

        updateTip ref cn@(CNode bmp arr) i = do
            let arr' = arrayUpdate (S k v) i arr
                cn'  = CNode bmp arr'
            ok <- compareAndSwap ref cn cn'
            unless ok $ go 0 undefined root

        extend ref cn@(CNode bmp arr) i k2 v2 lev = do
            inode2 <- newINode h (S k v) (hash k2) (S k2 v2) (nextLevel lev)
            let arr' = arrayUpdate (I inode2) i arr
                cn'  = CNode bmp arr'
            ok <- compareAndSwap ref cn cn'
            unless ok $ go 0 undefined root


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


remove :: (Eq k, Hashable k) => k -> Map k v -> IO ()
remove k (Map root) = go 0 undefined root
    where
        h = hash k
        go lev parent inode@(INode ref) = do
            main <- readIORef ref
            case main of
                cn@(CNode bmp arr) -> do
                    let m = mask h lev
                        i = sparseIndex bmp m
                    if bmp .&. m == 0
                        then return ()
                        else case arr !! i of
                            I inode2 -> go (nextLevel lev) inode inode2
                            S k2 v | k == k2 -> removeTip ref cn i m lev parent inode
                                   | otherwise -> return ()

                Tomb _ _ -> do
                    clean parent (prevLevel lev)
                    go 0 undefined root

        removeTip ref cn@(CNode bmp arr) i m lev parent inode = do
            let arr' = arrayDelete i arr
                cn'  = CNode (bmp `xor` m) arr'
                cn'' = contract cn' lev
            ok <- compareAndSwap ref cn cn'
            unless ok $ go 0 undefined root
            main <- readIORef ref
            when (isTomb main) $
                cleanParent parent inode h (prevLevel lev)

-----------------------------------------------------------------------
-- * Query

lookup :: (Eq k, Hashable k) => k -> Map k v -> IO (Maybe v)
lookup k (Map root) = go 0 undefined root
    where
        h = hash k
        go lev parent inode@(INode ref) = do
            main <- readIORef ref
            case main of
                cn@(CNode bmp arr) -> do
                    let m = mask h lev
                        i = sparseIndex bmp m
                    if bmp .&. m == 0
                        then return Nothing
                        else case arr !! i of
                            I inode2           -> go (nextLevel lev) inode inode2
                            S k2 v | k == k2   -> return (Just v)
                                   | otherwise -> return Nothing
                Tomb _ _ -> do
                    clean parent (prevLevel lev)
                    go 0 undefined root

-----------------------------------------------------------------------

clean :: INode k v -> Level -> IO ()
clean (INode ref) lev = do
    main <- readIORef ref
    case main of
        cn@(CNode _ _) -> do
            cn' <- compress cn lev
            compareAndSwap ref cn cn'
            return ()
        _ -> return ()

compress :: MainNode k v -> Level -> IO (MainNode k v)
compress cn@(CNode bmp arr) lev = do
    arr' <- mapM resurrect arr
    let cn' = CNode bmp arr'
    return $ contract cn' lev
compress x _ = return x

resurrect :: Branch k v -> IO (Branch k v)
resurrect i@(I (INode ref)) = do
    main <- readIORef ref
    case main of
        Tomb k v -> return (S k v)
        _        -> return i
resurrect s = return s

contract :: MainNode k v -> Level -> MainNode k v
contract cn@(CNode bmp [(S k v)]) lev | lev > 0 = Tomb k v
contract x _ = x

cleanParent :: INode k v -> INode k v -> Hash -> Level -> IO ()
cleanParent parent@(INode pref) inode@(INode ref) h lev = do
    main <- readIORef ref
    pmain <- readIORef pref
    case pmain of
        cn@(CNode bmp arr) -> do
            let m = mask h lev
                i = sparseIndex bmp m
            if bmp .&. m == 0
                then return ()
                else do
                    let sub = arr !! i
                    case sub of
                        (I subi) -> if not $ ptrEq subi inode
                            then return ()
                            else case main of
                                Tomb _ _ -> do
                                    cn' <- compress cn lev
                                    succ <- compareAndSwap pref cn cn'
                                    if not succ
                                        then cleanParent parent inode h lev
                                        else return ()
                        _ -> return ()
        _ -> return ()

-----------------------------------------------------------------------
-- * Lists

fromList :: (Eq k, Hashable k) => [(k,v)] -> IO (Map k v)
fromList xs = empty >>= \m -> mapM_ (\(k,v) -> insert k v m) xs >> return m

-- NOTE: 'unsafeToList' has no atomicity guarantees (meaning concurrent
-- changes to the map will lead to an inconsistent result) and probably
-- atrocious performance. It only exists so we can test some stuff before
-- proper snapshotting is implemented.
unsafeToList :: Map k v -> IO [(k,v)]
unsafeToList (Map inode) = go inode
    where
        go (INode ref) = readIORef ref >>= \(CNode bmp arr) -> go2 arr []
        go2 [] xs = return xs
        go2 ((I inode):arr) xs = go2 arr . (++ xs) =<< go inode
        go2 ((S k v):arr) xs = go2 arr ((k,v):xs)

-----------------------------------------------------------------------

-- see [Note: CAS and pointer equality]
compareAndSwap :: IORef a -> a -> a -> IO Bool
compareAndSwap ref old new =
    atomicModifyIORef' ref (\cur -> if cur `ptrEq` old
                                    then (new, True)
                                    else (cur, False))

ptrEq :: a -> a -> Bool
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

prevLevel :: Level -> Level
prevLevel = subtract bitsPerSubkey
{-# INLINE prevLevel #-}

-----------------------------------------------------------------------

-- TODO
arrayInsert :: a -> Int -> [a] -> [a]
arrayInsert x n xs = ys ++ [x] ++ zs
    where (ys,zs) = splitAt n xs

arrayUpdate :: a -> Int -> [a] -> [a]
arrayUpdate x n xs = ys ++ [x] ++ zs
    where (ys,_:zs) = splitAt n xs

arrayDelete :: Int -> [a] -> [a]
arrayDelete n xs = ys ++ zs
    where (ys,_:zs) = splitAt n xs

-----------------------------------------------------------------------

-- TODO
printMap :: (Show k, Show v) => Map k v -> IO ()
printMap (Map i) = goI i
    where
        goI (INode ref) = putStr "(I " >> readIORef ref >>= goM >> putStr ")\n"
        goM (CNode bmp arr) = do
            putStr $ "(C " ++ (show bmp) ++ " ["
            mapM_ (\b -> goB b >> putStr ", ") arr
            putStr $ "] )"
        goM (Tomb k v) = putStr $ "(T " ++ (show k) ++ " " ++ (show v) ++ ")"
        goB (I i) = putStr "\n" >> goI i
        goB (S k v) = putStr $ "(" ++ (show k) ++ "," ++ (show v) ++ ")"



