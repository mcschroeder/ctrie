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
    , delete
    , lookup
    , fromList
    , unsafeToList
    , printMap
    ) where

import Control.Applicative ((<$>))
import Control.Monad
import Control.Monad.Primitive
import Control.Monad.ST
import Data.Bits
import Data.Hashable (Hashable)
import qualified Data.Hashable as H
import Data.IORef
import qualified Data.List as List
import Data.Maybe
import Data.Primitive.Array
import Data.Word
import System.IO.Unsafe  -- see [Note: CAS and pointer equality]
import System.Mem.StableName
import Text.Printf
import Prelude hiding (lookup)

-----------------------------------------------------------------------

newtype Map   k v = Map (INode k v)
type    INode k v = IORef (MainNode k v)
data MainNode k v = CNode !Bitmap !(Array (Branch k v))
                  | Tomb !k v
                  | Collision ![(k,v)] -- !(Array (k,v)) ?
                    -- TODO: SNode data type for strict key field
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
empty = Map <$> newIORef (CNode 0 emptyArray)


-----------------------------------------------------------------------
-- * Modification

insert :: (Eq k, Hashable k) => k -> v -> Map k v -> IO ()
insert k v (Map root) = go 0 undefined root
    where
        h = hash k
        go lev parent inode = do
            main <- readIORef inode
            case main of
                cn@(CNode bmp arr) -> do
                    let m = mask h lev
                        i = sparseIndex bmp m
                    if bmp .&. m == 0
                        then insertTip inode cn i m
                        else case indexArray arr i of
                            I inode2 -> go (nextLevel lev) inode inode2
                            S k2 v2 | k == k2 -> updateTip inode cn i
                                    | otherwise -> extend inode cn i k2 v2 lev

                Tomb _ _ -> do
                    clean parent (prevLevel lev)
                    go 0 undefined root

                col@(Collision _) -> insertCollision inode col

        insertTip inode cn@(CNode bmp arr) i m = do
            let arr' = arrayInsert (S k v) i (popCount bmp) arr
                cn'  = CNode (bmp .|. m) arr'
            ok <- compareAndSwap inode cn cn'
            unless ok $ go 0 undefined root

        updateTip inode cn@(CNode bmp arr) i = do
            let arr' = arrayUpdate (S k v) i (popCount bmp) arr
                cn'  = CNode bmp arr'
            ok <- compareAndSwap inode cn cn'
            unless ok $ go 0 undefined root

        extend inode cn@(CNode bmp arr) i k2 v2 lev = do
            inode2 <- newINode h k v (hash k2) k2 v2 (nextLevel lev)
            let arr' = arrayUpdate (I inode2) i (popCount bmp) arr
                cn'  = CNode bmp arr'
            ok <- compareAndSwap inode cn cn'
            unless ok $ go 0 undefined root

        insertCollision inode col@(Collision xs) = do
            let col' = Collision $ (k,v) : List.filter ((/=) k . fst) xs
            ok <- compareAndSwap inode col col'
            unless ok $ go 0 undefined root


newINode :: Hash -> k -> v -> Hash -> k -> v -> Int -> IO (INode k v)
newINode h1 k1 v1 h2 k2 v2 lev
    | lev >= hashLength = newIORef $ Collision [(k1,v1),(k2,v2)]
    | otherwise = do
        let i1 = index h1 lev
            i2 = index h2 lev
            bmp = (unsafeShiftL 1 i1) .|. (unsafeShiftL 1 i2)
        case compare i1 i2 of
            LT -> newIORef $ CNode bmp $ arrayPair (S k1 v1) (S k2 v2)
            GT -> newIORef $ CNode bmp $ arrayPair (S k2 v2) (S k1 v1)
            EQ -> do inode' <- newINode h1 k1 v1 h2 k2 v2 (nextLevel lev)
                     newIORef $ CNode bmp $ singletonArray (I inode')


delete :: (Eq k, Hashable k) => k -> Map k v -> IO ()
delete k (Map root) = go 0 undefined root
    where
        h = hash k
        go lev parent inode = do
            main <- readIORef inode
            case main of
                cn@(CNode bmp arr) -> do
                    let m = mask h lev
                        i = sparseIndex bmp m
                    if bmp .&. m == 0
                        then return ()
                        else case indexArray arr i of
                            I inode2 -> go (nextLevel lev) inode inode2
                            S k2 v | k == k2 -> removeTip parent inode cn i m lev
                                   | otherwise -> return ()

                Tomb _ _ -> do
                    clean parent (prevLevel lev)
                    go 0 undefined root

                col@(Collision _) -> removeCollision inode col

        removeTip parent inode cn@(CNode bmp arr) i m lev = do
            let arr' = arrayDelete i (popCount bmp) arr
                cn'  = CNode (bmp `xor` m) arr'
                cn'' = contract lev cn'
            ok <- compareAndSwap inode cn cn'
            unless ok $ go 0 undefined root
            main <- readIORef inode
            when (isTomb main) $
                cleanParent parent inode h (prevLevel lev)

        removeCollision inode col@(Collision xs) = do
            let xs'  = filter ((/=) k . fst) xs
                col' = if length xs' > 1
                            then Collision xs'
                            else let (k0,v0) = head xs'
                                 in Tomb k0 v0
            ok <- compareAndSwap inode col col'
            unless ok $ go 0 undefined root

-----------------------------------------------------------------------
-- * Query

lookup :: (Eq k, Hashable k) => k -> Map k v -> IO (Maybe v)
lookup k (Map root) = go 0 undefined root
    where
        h = hash k
        go lev parent inode = do
            main <- readIORef inode
            case main of
                cn@(CNode bmp arr) -> do
                    let m = mask h lev
                        i = sparseIndex bmp m
                    if bmp .&. m == 0
                        then return Nothing
                        else case indexArray arr i of
                            I inode2           -> go (nextLevel lev) inode inode2
                            S k2 v | k == k2   -> return (Just v)
                                   | otherwise -> return Nothing

                Tomb _ _ -> do
                    clean parent (prevLevel lev)
                    go 0 undefined root

                Collision xs -> return $ List.lookup k xs

-----------------------------------------------------------------------
-- * Internal compression operations

clean :: INode k v -> Level -> IO ()
clean inode lev = do
    main <- readIORef inode
    case main of
        cn@(CNode _ _) -> do
            cn' <- compress lev cn
            void $ compareAndSwap inode cn cn'
        _ -> return ()

cleanParent :: INode k v -> INode k v -> Hash -> Level -> IO ()
cleanParent parent inode h lev = do
    pmain <- readIORef parent
    case pmain of
        cn@(CNode bmp arr) -> do
            let m = mask h lev
                i = sparseIndex bmp m
            unless (bmp .&. m == 0) $
                case indexArray arr i of
                    I inode2 | inode2 == inode -> do
                        main <- readIORef inode
                        when (isTomb main) $ do
                            cn' <- compress lev cn
                            ok  <- compareAndSwap parent cn cn'
                            unless ok $ cleanParent parent inode h lev
                    _ -> return ()
        _ -> return ()

compress :: Level -> MainNode k v -> IO (MainNode k v)
compress lev (CNode bmp arr) =
    contract lev <$> CNode bmp <$> arrayMapM resurrect (popCount bmp) arr
compress _ x = return x

resurrect :: Branch k v -> IO (Branch k v)
resurrect b@(I inode) = do
    main <- readIORef inode
    case main of
        Tomb k v -> return (S k v)
        _        -> return b
resurrect b = return b

contract :: Level -> MainNode k v -> MainNode k v
contract lev (CNode bmp arr) | lev > 0
                           , popCount bmp == 1
                           , S k v <- arrayHead arr
                           = Tomb k v
contract _ x = x

-----------------------------------------------------------------------
-- * Lists

fromList :: (Eq k, Hashable k) => [(k,v)] -> IO (Map k v)
fromList xs = empty >>= \m -> mapM_ (\(k,v) -> insert k v m) xs >> return m

-- NOTE: 'unsafeToList' has no atomicity guarantees (meaning concurrent
-- changes to the map will lead to an inconsistent result) and probably
-- atrocious performance. It only exists so we can test some stuff before
-- proper snapshotting is implemented.
unsafeToList :: Map k v -> IO [(k,v)]
unsafeToList (Map root) = go root
    where
        go inode = do
            main <- readIORef inode
            case main of
                CNode bmp arr -> arrayFoldM' go2 [] (popCount bmp) arr
                Tomb k v -> return [(k,v)]
                Collision xs -> return xs

        go2 xs (I inode) = go inode >>= \ys -> return (ys ++ xs)
        go2 xs (S k v) = return $ (k,v) : xs

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

hashLength :: Int
hashLength = bitSize (undefined :: Word)

bitsPerSubkey :: Int
bitsPerSubkey = floor . logBase 2 . fromIntegral . bitSize $ hashLength

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

arrayInsert :: a -> Int -> Int -> Array a -> Array a
arrayInsert x i n arr = runST $ do
    marr <- newArray (n+1) (error "arrayInsert")
    copyArray marr 0 arr 0 i
    writeArray marr i x
    copyArray marr (i+1) arr i (n-i)
    unsafeFreezeArray marr
{-# INLINE arrayInsert #-}

arrayUpdate :: a -> Int -> Int -> Array a -> Array a
arrayUpdate x i n arr = runST $ do
    marr <- newArray n (error "arrayUpdate")
    copyArray marr 0 arr 0 n
    writeArray marr i x
    unsafeFreezeArray marr
{-# INLINE arrayUpdate #-}

arrayDelete :: Int -> Int -> Array a -> Array a
arrayDelete i n arr = runST $ do
    marr <- newArray (n-1) (error "arrayDelete")
    copyArray marr 0 arr 0 i
    copyArray marr i arr (i+1) (n-(i+1))
    unsafeFreezeArray marr
{-# INLINE arrayDelete #-}

arrayPair :: a -> a -> Array a
arrayPair x y = runST $ do
    marr <- newArray 2 (error "arrayPair")
    writeArray marr 0 x
    writeArray marr 1 y
    unsafeFreezeArray marr
{-# INLINE arrayPair #-}

arrayMapM :: PrimMonad m => (a -> m b) -> Int -> Array a -> m (Array b)
arrayMapM f n arr = do
    marr <- newArray n (error "arrayMapM")
    go arr marr 0 n
    unsafeFreezeArray marr
    where
        go arr marr i n
            | i >= n = return ()
            | otherwise = do
                x <- indexArrayM arr i
                writeArray marr i =<< f x
                go arr marr (i+1) n
{-# INLINE arrayMapM #-}

arrayMapM_ :: PrimMonad m => (a -> m b) -> Int -> Array a -> m ()
arrayMapM_ f n arr = go arr 0 n
    where
        go arr i n
            | i >= n = return ()
            | otherwise = do
                x <- indexArrayM arr i
                _ <- f x
                go arr (i+1) n
{-# INLINE arrayMapM_ #-}

arrayFoldM' :: PrimMonad m => (b -> a -> m b) -> b -> Int -> Array a -> m b
arrayFoldM' f z0 n arr0 = go arr0 n 0 z0
    where
        go arr n i !z
            | i >= n = return z
            | otherwise = do
                x <- indexArrayM arr i
                go arr n (i+1) =<< f z x
{-# INLINE arrayFoldM' #-}

arrayHead :: Array a -> a
arrayHead = flip indexArray 0
{-# INLINE arrayHead #-}

emptyArray :: Array a
emptyArray = runST $ unsafeFreezeArray =<< newArray 0 (error "empty array")
{-# INLINE emptyArray #-}

singletonArray :: a -> Array a
singletonArray x = runST $ do
    marr <- newArray 1 x
    unsafeFreezeArray marr
{-# INLINE singletonArray #-}

-----------------------------------------------------------------------

-- TODO
printMap :: (Show k, Show v) => Map k v -> IO ()
printMap (Map root) = goI root
    where
        goI inode = putStr "(I " >> readIORef inode >>= goM >> putStr ")\n"
        goM (CNode bmp arr) = do
            putStr $ "(C " ++ (show bmp) ++ " ["
            arrayMapM_ (\b -> goB b >> putStr ", ") (popCount bmp) arr
            putStr $ "] )"
        goM (Tomb k v) = putStr $ "(T " ++ (show k) ++ " " ++ (show v) ++ ")"
        goM (Collision xs) = putStr $ "(Collision " ++ show xs ++ ")"
        goB (I i) = putStr "\n" >> goI i
        goB (S k v) = putStr $ "(" ++ (show k) ++ "," ++ (show v) ++ ")"
