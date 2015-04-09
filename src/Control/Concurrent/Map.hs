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
import qualified Data.List as List
import Data.Maybe
import Prelude hiding (lookup)

import Data.SparseArray

-----------------------------------------------------------------------

-- | A map from keys @k@ to values @v@.
newtype Map k v = Map (INode k v)

type INode k v = IORef (MainNode k v)

data MainNode k v = CNode !(SparseArray (Branch k v))
                  | Tomb !(SNode k v)
                  | Collision ![SNode k v]

data Branch k v = INode !(INode k v)
                | SNode !(SNode k v)

data SNode k v = S !k v
    deriving (Eq, Show)

isTomb :: MainNode k v -> Bool
isTomb (Tomb _) = True
isTomb _        = False

-----------------------------------------------------------------------
-- * Construction

-- | /O(1)/. Construct an empty map.
empty :: IO (Map k v)
empty = Map <$> newIORef (CNode emptyArray)


-----------------------------------------------------------------------
-- * Modification

-- | /O(log n)/. Associate the given value with the given key.
-- If the key is already present in the map, the old value is replaced.
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> IO ()
insert k v (Map root) = go0
    where
        h = hash k
        go0 = go 0 undefined root
        go lev parent inode = do
            ticket <- readForCAS inode
            case peekTicket ticket of
                CNode a -> case arrayLookup lev h a of
                    Just (INode inode2) -> go (down lev) inode inode2
                    Just (SNode (S k2 v2))
                        | k == k2 -> do
                            let a' = arrayUpdate lev h (SNode (S k v)) a
                                cn' = CNode a'
                            unlessM (fst <$> casIORef inode ticket cn') go0
                        | otherwise -> do
                            let h2 = hash k2
                            inode2 <- newINode h k v h2 k2 v2 (down lev)
                            let a' = arrayUpdate lev h (INode inode2) a
                                cn'  = CNode a'
                            unlessM (fst <$> casIORef inode ticket cn') go0
                    Nothing -> do
                        let a' = arrayInsert lev h (SNode (S k v)) a
                            cn' = CNode a'
                        unlessM (fst <$> casIORef inode ticket cn') go0

                Tomb _ -> clean parent (up lev) >> go0

                Collision arr -> do
                    let arr' = S k v : filter (\(S k2 _) -> k2 /= k) arr
                        col' = Collision arr'
                    unlessM (fst <$> casIORef inode ticket col') go0

{-# INLINABLE insert #-}

newINode :: Hash -> k -> v -> Hash -> k -> v -> Int -> IO (INode k v)
newINode h1 k1 v1 h2 k2 v2 lev
    | lev >= lastLevel = newIORef $ Collision [S k1 v1, S k2 v2]
    | otherwise = do
        case mkPair lev h1 (SNode (S k1 v1)) h2 (SNode (S k2 v2)) of
            Just pair -> newIORef (CNode pair)
            Nothing -> do
                inode' <- newINode h1 k1 v1 h2 k2 v2 (down lev)
                let a = mkSingleton lev h1 (INode inode')
                newIORef (CNode a)


-- | /O(log n)/. Remove the given key and its associated value from the map,
-- if present.
delete :: (Eq k, Hashable k) => k -> Map k v -> IO ()
delete k (Map root) = go0
    where
        h = hash k
        go0 = go 0 undefined root
        go lev parent inode = do
            ticket <- readForCAS inode
            case peekTicket ticket of
                CNode a -> case arrayLookup lev h a of
                    Just (INode inode2) -> go (down lev) inode inode2
                    Just (SNode (S k2 _))
                        | k == k2 -> do
                            let a' = arrayDelete lev h a
                                cn' = contract lev (CNode a')
                            unlessM (fst <$> casIORef inode ticket cn') go0
                            whenM (isTomb <$> readIORef inode) $
                                        cleanParent parent inode h (up lev)

                        | otherwise -> return () -- not found

                    Nothing -> return ()  -- not found

                Tomb _ -> clean parent (up lev) >> go0

                Collision arr -> do
                    let arr' = filter (\(S k2 _) -> k2 /= k) $ arr
                        col' | [s] <- arr' = Tomb s
                             | otherwise   = Collision arr'
                    unlessM (fst <$> casIORef inode ticket col') go0

{-# INLINABLE delete #-}

-----------------------------------------------------------------------
-- * Query

-- | /O(log n)/. Return the value associated with the given key, or 'Nothing'.
lookup :: (Eq k, Hashable k) => k -> Map k v -> IO (Maybe v)
lookup k (Map root) = go0
    where
        h = hash k
        go0 = go 0 undefined root
        go lev parent inode = do
            main <- readIORef inode
            case main of
                CNode a -> case arrayLookup lev h a of
                    Just (INode inode2) -> go (down lev) inode inode2
                    Just (SNode (S k2 v)) | k == k2 -> return (Just v)
                                          | otherwise -> return Nothing
                    Nothing -> return Nothing

                Tomb _ -> clean parent (up lev) >> go0

                Collision xs -> do
                    case List.find (\(S k2 _) -> k2 == k) xs of
                        Just (S _ v) -> return (Just v)
                        _            -> return Nothing

{-# INLINABLE lookup #-}

-----------------------------------------------------------------------
-- * Internal compression operations

clean :: INode k v -> Level -> IO ()
clean inode lev = do
    ticket <- readForCAS inode
    case peekTicket ticket of
        cn@(CNode _) -> do
            cn' <- compress lev cn
            void $ casIORef inode ticket cn'
        _ -> return ()
{-# INLINE clean #-}

cleanParent :: INode k v -> INode k v -> Hash -> Level -> IO ()
cleanParent parent inode h lev = do
    ticket <- readForCAS parent
    case peekTicket ticket of
        cn@(CNode a) -> case arrayLookup lev h a of
            Just (INode inode2) | inode2 == inode ->
                whenM (isTomb <$> readIORef inode) $ do
                    cn' <- compress lev cn
                    unlessM (fst <$> casIORef parent ticket cn') $
                        cleanParent parent inode h lev
            _ -> return ()
        _ -> return ()

compress :: Level -> MainNode k v -> IO (MainNode k v)
compress lev (CNode a) = contract lev . CNode <$> arrayMapM resurrect a
compress _ x = return x
{-# INLINE compress #-}

resurrect :: Branch k v -> IO (Branch k v)
resurrect b@(INode inode) = do
    main <- readIORef inode
    case main of
        Tomb s -> return (SNode s)
        _      -> return b
resurrect b = return b
{-# INLINE resurrect #-}

contract :: Level -> MainNode k v -> MainNode k v
contract lev (CNode a) | lev > 0
                       , Just (SNode s) <- arrayToMaybe a
                       = Tomb s
contract _ x = x
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
                CNode a -> arrayFoldM' go2 [] a
                Tomb (S k v) -> return [(k,v)]
                Collision xs -> return $ map (\(S k v) -> (k,v)) xs

        go2 xs (INode inode) = go inode >>= \ys -> return (ys ++ xs)
        go2 xs (SNode (S k v)) = return $ (k,v) : xs
{-# INLINABLE unsafeToList #-}

-----------------------------------------------------------------------

whenM :: Monad m => m Bool -> m () -> m ()
whenM p s = p >>= \t -> if t then s else return ()
{-# INLINE whenM #-}

unlessM :: Monad m => m Bool -> m () -> m ()
unlessM p s = p >>= \t -> if t then return () else s
{-# INLINE unlessM #-}
