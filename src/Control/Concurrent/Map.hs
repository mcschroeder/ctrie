{-# LANGUAGE BangPatterns, PatternGuards, MagicHash #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
-----------------------------------------------------------------------
-- | A non-blocking concurrent map from hashable keys to values.
--
-- The implementation is based on /lock-free concurrent hash tries/
-- (aka /Ctries/) as described by:
--
--    * Aleksander Prokopec, Phil Bagwell, Martin Odersky,
--      \"/Cache-Aware Lock-Free Concurent Hash Tries/\"
--    * Aleksander Prokopec, Nathan G. Bronson, Phil Bagwell, Martin
--      Odersky \"/Concurrent Tries with Efficient Non-Blocking Snapshots/\"
--
-- Operations have a worst-case complexity of /O(log n)/, with a base
-- equal to the size of the native 'Word'.
--
-----------------------------------------------------------------------

module Control.Concurrent.Map
    ( Map

      -- * Construction
    , empty

      -- * Modification
    , insert
    , delete

      -- * Query
    , lookup

      -- * Lists
    , fromList
    , unsafeToList
    ) where

import Control.Applicative ((<$>))
import Control.Monad
import Data.Atomics
import Data.IORef
import Data.Maybe
import Prelude hiding (lookup)

import Data.SparseArray

-----------------------------------------------------------------------

-- | A map from keys @k@ to values @v@.
newtype Map k v = Map (INode k v)

type INode k v = IORef (Node k v)

data Node k v = Array !(SparseArray (Branch k v))
              | List  ![Leaf k v]
              | Tomb  !(Leaf k v)

data Branch k v = I !(INode k v)
                | L !(Leaf k v)

data Leaf k v = Leaf !k v
    deriving (Eq, Show)

-----------------------------------------------------------------------
-- * Construction

-- | /O(1)/. Construct an empty map.
empty :: IO (Map k v)
empty = Map <$> newIORef (Array emptyArray)

-----------------------------------------------------------------------
-- * Modification

-- | /O(log n)/. Associate the given value with the given key.
-- If the key is already present in the map, the old value is replaced.
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> IO ()
insert k v (Map root) = go root 0 undefined
  where
    h = hash k
    leaf = Leaf k v
    go inode level parent = do
        ticket <- readForCAS inode
        let cas node = do (ok,_) <- casIORef inode ticket node
                          unless ok (go root 0 undefined)
        case peekTicket ticket of
            Array a -> case arrayLookup level h a of
                Just (I inode2) -> go inode2 (down level) inode
                Just (L leaf2@(Leaf k2 _))
                    | k == k2   -> cas $ Array (arrayUpdate level h (L leaf) a)
                    | otherwise -> cas =<< growTrie level leaf2 a
                Nothing         -> cas $ Array (arrayInsert level h (L leaf) a)

            List xs -> cas $ List (listInsert leaf xs)

            Tomb _  -> clean parent (up level) >> go root 0 undefined

    growTrie level leaf2@(Leaf k2 _) a = do
        inode2 <- combineLeaves (down level) (hash k2) leaf2
        return $ Array (arrayUpdate level h (I inode2) a)

    combineLeaves level h2 leaf2
        | level >= lastLevel = newIORef (List [leaf, leaf2])
        | otherwise = do
            case mkPair level h (L leaf) h2 (L leaf2) of
                Just pair -> newIORef (Array pair)
                Nothing -> do
                    inode <- combineLeaves (down level) h2 leaf2
                    let a = mkSingleton level h (I inode)
                    newIORef (Array a)

{-# INLINABLE insert #-}


-- | /O(log n)/. Remove the given key and its associated value from the map,
-- if present.
delete :: (Eq k, Hashable k) => k -> Map k v -> IO ()
delete k m@(Map root) = do
    ok <- go root 0 undefined
    unless ok (delete k m)
  where
    h = hash k
    go inode level parent = do
        ticket <- readForCAS inode
        case peekTicket ticket of
            Array a -> do
                ok <- case arrayLookup level h a of
                    Just (I inode2) -> go inode2 (down level) inode
                    Just (L (Leaf k2 _))
                        | k == k2   -> casArrayDelete inode ticket level a
                        | otherwise -> return True
                    Nothing         -> return True
                when ok (compressIfPossible level inode parent)
                return ok
            List xs -> casListDelete inode ticket xs
            Tomb _  -> clean parent (up level) >> go root 0 undefined

    compressIfPossible level inode parent = do
        n <- readIORef inode
        case n of
            Tomb _ -> cleanParent parent inode h (up level)
            _      -> return ()

    casArrayDelete inode ticket level a = do
        let a' = arrayDelete level h a
            n  = contract level (Array a')
        (ok,_) <- casIORef inode ticket n
        return ok

    casListDelete inode ticket xs = do
        let xs' = listDelete k xs
            n | [l] <- xs' = Tomb l
              | otherwise  = List xs'
        (ok,_) <- casIORef inode ticket n
        return ok

{-# INLINABLE delete #-}

-----------------------------------------------------------------------
-- * Query

-- | /O(log n)/. Return the value associated with the given key, or 'Nothing'.
lookup :: (Eq k, Hashable k) => k -> Map k v -> IO (Maybe v)
lookup k (Map root) = go root 0 undefined
  where
    h = hash k
    go inode level parent = do
        node <- readIORef inode
        case node of
            Array a -> case arrayLookup level h a of
                Just (I inode2) -> go inode2 (down level) inode
                Just (L (Leaf k2 v))
                    | k == k2   -> return (Just v)
                    | otherwise -> return Nothing
                Nothing         -> return Nothing
            List xs -> return $ listLookup k xs
            Tomb _  -> clean parent (up level) >> go root 0 undefined

{-# INLINABLE lookup #-}

-----------------------------------------------------------------------
-- * Internal compression operations

clean :: INode k v -> Level -> IO ()
clean inode level = do
    ticket <- readForCAS inode
    case peekTicket ticket of
        n@(Array _) -> do
            n' <- compress level n
            void $ casIORef inode ticket n'
        _ -> return ()
{-# INLINE clean #-}

cleanParent :: INode k v -> INode k v -> Hash -> Level -> IO ()
cleanParent parent inode h level = do
    ticket <- readForCAS parent
    case peekTicket ticket of
        n@(Array a) -> case arrayLookup level h a of
            Just (I inode2) | inode2 == inode -> do
                n2 <- readIORef inode
                case n2 of
                    Tomb _ -> do
                        n' <- compress level n
                        (ok,_) <- casIORef parent ticket n'
                        unless ok $ cleanParent parent inode h level
                    _ -> return ()
            _ -> return ()
        _ -> return ()

compress :: Level -> Node k v -> IO (Node k v)
compress level (Array a) = contract level . Array <$> arrayMapM resurrect a
compress _     n         = return n
{-# INLINE compress #-}

resurrect :: Branch k v -> IO (Branch k v)
resurrect b@(I inode) = do n <- readIORef inode
                           case n of
                               Tomb leaf -> return (L leaf)
                               _         -> return b
resurrect b           = return b
{-# INLINE resurrect #-}

contract :: Level -> Node k v -> Node k v
contract level (Array a) | level > 0
                         , Just (L s) <- arrayToMaybe a
                         = Tomb s
contract _     n         = n
{-# INLINE contract #-}

-----------------------------------------------------------------------
-- * Lists

-- | /O(n * log n)/. Construct a map from a list of key/value pairs.
fromList :: (Eq k, Hashable k) => [(k,v)] -> IO (Map k v)
fromList xs = empty >>= \m -> mapM_ (\(k,v) -> insert k v m) xs >> return m
{-# INLINABLE fromList #-}

-- | /O(n)/. Unsafely convert the map to a list of key/value pairs.
--
-- WARNING: 'unsafeToList' makes no atomicity guarantees. Concurrent
-- changes to the map will lead to inconsistent results.
unsafeToList :: Map k v -> IO [(k,v)]
unsafeToList (Map root) = go root
  where
    go inode = do
        main <- readIORef inode
        case main of
            Array a -> arrayFoldM' go2 [] a
            List xs -> return $ map (\(Leaf k v) -> (k,v)) xs
            Tomb (Leaf k v) -> return [(k,v)]

    go2 xs (I inode) = go inode >>= \ys -> return (ys ++ xs)
    go2 xs (L (Leaf k v)) = return $ (k,v) : xs
{-# INLINABLE unsafeToList #-}

-----------------------------------------------------------------------

listLookup :: Eq k => k -> [Leaf k v] -> Maybe v
listLookup k1 = go
  where
    go []                           = Nothing
    go (Leaf k2 v : xs) | k1 == k2  = Just v
                        | otherwise = go xs

listInsert :: Eq k => Leaf k v -> [Leaf k v] -> [Leaf k v]
listInsert y@(Leaf k1 _) = go
  where
    go []                             = [y]
    go (x@(Leaf k2 _):xs) | k1 == k2  = y : xs
                          | otherwise = x : go xs

listDelete :: Eq k => k -> [Leaf k v] -> [Leaf k v]
listDelete k1 = go
  where
    go []                             = []
    go (x@(Leaf k2 _):xs) | k1 == k2  = xs
                          | otherwise = x : go xs
