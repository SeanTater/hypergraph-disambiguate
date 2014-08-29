{-# LANGUAGE BangPatterns, FlexibleInstances #-} -- Data.Binary uses this anyway
module RIndex.RIndex where
{-
    You'll need:
        murmur-hash
        vector
-}
import Prelude hiding (mapM_, minimum)
import Data.Word
import Data.Int
import Data.Foldable
import Data.Bits
import Data.Binary
import Control.Monad (when, unless, liftM)
import Control.Monad.Primitive
import Control.Monad.ST
import Control.Parallel
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.Map.Strict as M
import qualified Data.Digest.Murmur64 as Murmur
import qualified Data.Sequence as Seq

-- -- Utility

applyMV :: (PrimMonad m, V.Unbox t) => (Int -> t -> t) -> VM.MVector (PrimState m) t -> Int -> m ()
applyMV func vec i = do
    e_in <- VM.read vec i
    VM.write vec i (func i e_in)
    return ()


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


instance Binary (V.Vector Int32) where
    put x = put $ V.toList x
    get = liftM V.fromList get

{-| Get murmur-based hashes of a string.
Multiple hashes are made by replacing the seed value with
integers from 1 to n.
-}
getNHashes n word =
    [h seed | seed <- [1..n]]
    where h seed = fromIntegral $ Murmur.asWord64 $ Murmur.hash64WithSeed seed word

-- | Monadically add a word to a hash
hashWordIntoContext :: (PrimMonad m) => MutableContext m -> String -> m ()
hashWordIntoContext context word =
    mapM_ (applyMV (picksign) context) indices
    where
        picksign x _ = fromIntegral $ (popCount x .&. 1) * 2 - 1
        indices = map (`mod` context_dims) $ getNHashes word_dims word

-- | Generate and sum contexts for words in a list of tokens
hashChunk :: [String] -> Context
hashChunk tokens = runST $ do
    vec <- VM.replicate context_dims 0
    mapM_ (hashWordIntoContext vec) tokens
    V.freeze vec


-- -- Bloom
    
{-
 - Simple counting bloom filter
 - It will add a word to a filter until it reaches the bloom_threshold
 - Then it will no longer add (to avoid overflow) but will return popular=True
 -}
bloom_hash_count = 10
bloom_bin_count = 25000000
bloom_threshold = 50 :: Word8

-- | Add a token to a monadic bloom filter
-- and return whether the token has passed the popularity threshold
addToBloom :: (PrimMonad m) => MutableBloom m -> String -> m ()
addToBloom bloom word = do
    counts <- sequence [ VM.read bloom i | i <- indices ]
    let popular = (minimum counts) >= bloom_threshold
    when popular $
        mapM_ (applyMV (incrementClamp) bloom) indices
    where
        incrementClamp _ a = max a (a+1)
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]

isPopular :: Bloom -> String -> Bool
isPopular bloom word = do
    (minimum [(V.!) bloom i | i <- indices ]) >= bloom_threshold
    where
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]

mergeBlooms :: Bloom -> Bloom -> Bloom
mergeBlooms x y =
    par x $ seq y $ V.zipWith addClamp x y
    where addClamp a b = max 255 (a+b)
        
makeNewBloom :: PrimMonad m => m (VM.MVector (PrimState m) Word8)
makeNewBloom =
    VM.replicate bloom_bin_count (0::Word8)

makeImmBloom :: Bloom
makeImmBloom = V.replicate bloom_bin_count 0
    

-- | Monadic Bloom container
type MutableBloom m = VM.MVector (PrimState m) Word8
-- | Bloom container
type Bloom = V.Vector Word8
-- | Monadic Bloom with associated map
type ContextMap = M.Map String Context

countChunk :: Foldable f => f [String] -> Bloom
countChunk paragraphs = runST $ do
    bloom <- makeNewBloom
    mapM_ (mapM_ (addToBloom bloom)) paragraphs
    V.freeze bloom

-- -- Random Indexing

-- | Insert or merge a word's context into the main (pure) map,
-- if the word has passed the popularity threshold in the bloom
addWordContext :: Context -> Bloom -> ContextMap -> String -> ContextMap
addWordContext new_context bloom context_map word =
    if isPopular bloom word
       then M.insertWith (V.zipWith (+)) word new_context context_map
       else context_map

-- | Chain addWordContext, but only calculate paragraph context once (used by binaryChunkContext)
-- Note that every word is counted in the context, _even the unpopular ones_.
-- Whether this is good or not is up for debate
addBinaryMultiwordContext :: Bloom -> ContextMap -> [String] -> ContextMap
addBinaryMultiwordContext bloom context_map tokens =
    foldl' (addWordContext chunk_context bloom) context_map tokens
    where
        chunk_context = hashChunk tokens

-- | Fill contexts based on whether words cooccur in a chunk
indexBinaryChunks :: Bloom -> ContextMap -> (Seq.Seq [String]) -> ContextMap
indexBinaryChunks bloom context_map tokenized_chunks =
    foldl' (addBinaryMultiwordContext bloom) context_map tokenized_chunks
