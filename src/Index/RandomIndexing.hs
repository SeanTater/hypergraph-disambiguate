{-# LANGUAGE BangPatterns, FlexibleInstances #-} -- Data.Binary uses this anyway
module Index.RandomIndexing (
    stat,
    ContextMap(..),
    addBinaryMultiwordContext) where
{-
    You'll need:
        murmur-hash
        vector
-}
import Prelude hiding (mapM_, minimum)
import Data.Int
import Data.Foldable
import Data.Bits (popCount, (.&.))
import Data.Binary
import Data.Monoid
import Control.Monad (when, unless, liftM)
import Control.Monad.Primitive
import Control.Parallel
import Control.Monad.ST
import qualified Data.Text as Text
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.HashMap.Strict as M
import qualified Data.Sequence as Seq
import Index.Bloom
import Index.Utility

import Debug.Trace

-- | n-dimensional space in which words are placed.
-- more dimensions -> less overlap, slower
context_dims = 1024
-- | count of dimensions where a word is not on the axis.
-- more -> more partial collisions, less total collisions
word_dims = 20

-- | Signed type used to store contexts (balance between range and memory)
type ContextSize = Int32

-- | Mutable context implemented as an MVector
type MutableContext m = VM.MVector (PrimState m) ContextSize

-- | Immutable context
newtype Context = Context (V.Vector ContextSize)

newtype ContextMap = ContextMap (M.HashMap Text.Text Context)

-- | Contexts are mergable
instance Monoid Context where
    mempty = Context $ V.replicate context_dims 0
    mappend (Context n1) (Context n2) =
        Context $ V.zipWith (+) n1 n2
    
-- | ContextMaps are mergable
instance Monoid ContextMap where
    mempty = ContextMap M.empty
    mappend (ContextMap !a) (ContextMap !b) =
        ContextMap $ traceShow (M.size a) $ M.unionWith mappend a b

-- | Contexts can be encoded as binary arrays
instance Binary Context where
    put (Context x) = put $ V.toList x
    get = liftM (Context . V.fromList) get


-- | Monadically add a word to a hash
hashWordIntoContext :: (PrimMonad m) => MutableContext m -> Text.Text -> m ()
hashWordIntoContext context word =

    mapM_ (applyMV (picksign) context) indices
    where
        picksign x _ = fromIntegral $ (popCount x .&. 1) * 2 - 1
        indices = map (`mod` context_dims) $ getNHashes word_dims word

-- | Generate and sum contexts for words in a list of tokens
hashChunk :: [Text.Text] -> Context
hashChunk tokens = runST $ do
    vec <- VM.replicate context_dims 0
    mapM_ (hashWordIntoContext vec) tokens
    fvec <- V.freeze vec
    return $ Context fvec

-- Semantic Distribution Indexing

-- | Insert or merge a word's context into the main (pure) map,
-- if the word has passed the popularity threshold in the bloom
addWordContext :: Context -> Bloom -> ContextMap -> Text.Text -> ContextMap
addWordContext new_context bloom (ContextMap context_map) word =
    if isPopular bloom word
       then ContextMap $ M.insertWith mappend word new_context context_map
       else ContextMap context_map

-- | Chain addWordContext, but only calculate paragraph context once (used by binaryChunkContext)
-- Note that every word is counted in the context, _even the unpopular ones_.
-- Whether this is good or not is up for debate
addBinaryMultiwordContext :: Bloom -> ContextMap -> [Text.Text] -> ContextMap
addBinaryMultiwordContext bloom context_map tokens =
    foldl' (addWordContext chunk_context bloom) context_map tokens
    where
        chunk_context = hashChunk tokens

-- | Fill contexts based on whether words cooccur in a chunk
indexBinaryChunks :: Bloom -> ContextMap -> (Seq.Seq [Text.Text]) -> ContextMap
indexBinaryChunks bloom context_map tokenized_chunks =
    foldl' (addBinaryMultiwordContext bloom) context_map tokenized_chunks

    
stat :: ContextMap -> String
stat (ContextMap cm) =
    "Context Map Statistics: "
        ++ show sz ++ " keys, about" ++ show (sz * 8 / 1024) ++ " MB"
    where
        sz = fromIntegral $ M.size cm