{-# LANGUAGE BangPatterns, FlexibleInstances #-} -- Data.Binary uses this anyway
module Index.RandomIndexing where
{-
    You'll need:
        murmur-hash
        vector
-}
import Prelude hiding (mapM_, minimum)
import Data.Int
import Data.Foldable
import Data.Bits
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

-- -- Hashing, for use in Bloom and in Random Indexing

-- | n-dimensional space in which words are placed.
-- more dimensions -> less overlap, less performance
context_dims = 1024
-- | count of dimensions where a word is not on the axis.
-- more -> more partial collisions, less total collisions
word_dims = 20

-- | Signed type used to store contexts (balance between range and memory)
type ContextSize = Int32
-- | Mutable context (implemented as an MVector
type MutableContext m = VM.MVector (PrimState m) ContextSize
-- | Immutable context
type Context = V.Vector ContextSize        

newtype ContextMap = ContextMap (M.HashMap Text.Text Context)
instance Monoid ContextMap where
    mempty = ContextMap M.empty
    mappend (ContextMap !a) (ContextMap !b) =
        ContextMap $ traceShow (M.size a) $ M.unionWith (V.zipWith (+)) a b

instance Binary (V.Vector Int32) where
    put x = put $ V.toList x
    get = liftM V.fromList get



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
    V.freeze vec
            

-- -- Random Indexing

-- | Insert or merge a word's context into the main (pure) map,
-- if the word has passed the popularity threshold in the bloom
addWordContext :: Context -> Bloom -> ContextMap -> Text.Text -> ContextMap
addWordContext new_context bloom (ContextMap context_map) word =
    if isPopular bloom word
       then ContextMap $ M.insertWith (V.zipWith (+)) word new_context context_map
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
