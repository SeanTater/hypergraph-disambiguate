module RIndex.RIndex where
{-
    You'll need:
        murmur-hash
        vector
-}
import Data.Word
import Data.Int
import Data.List
import Control.Monad
import Control.Monad.Primitive
import Control.Monad.ST
--import Data.Array.ST
--import Data.Array.Unboxed
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.Map.Strict as M
import qualified Data.Digest.Murmur64 as Murmur

-- -- Utility

-- Use modify to get around state

--vmap :: (VM.Unbox a) => (a -> a) -> [Int] -> VM.MVector s a -> ST s ()
vmap :: (PrimMonad m, V.Unbox t) => (t -> t) -> [Int] -> VM.MVector (PrimState m) t -> m ()
vmap func indices ivec =
    vimap (\_ b -> func b) indices ivec

--vimap :: (VM.Unbox a) => (Int -> a -> a) -> [Int] -> VM.MVector s a -> ST s ()
vimap :: (PrimMonad m, V.Unbox t) => (Int -> t -> t) -> [Int] -> VM.MVector (PrimState m) t -> m ()
vimap func indices vec =
    foldM_ (map1) vec indices
    where
         map1 ov i = do
             e_in <- VM.read ov i
             let e_out = func i e_in
             VM.write ov i e_out
             return ov



-- -- Hashing, for use in Bloom and in Random Indexing

-- Feel free to set these
-- context_dims: n-dimensional space in which words are placed
--               more dimensions -> less overlap, less performance
context_dims = 1024 :: Int
-- word_dims: count of dimensions where a word is not on the axis
--            more -> more partial collisions, less total collisions
word_dims = 20
-- Balance range with memory using by changing this type
context_type = 0 :: Int64

{- Get the hash of a word (or any String)
 -
 - This vector is derived from repeated Murmur hashes
 - this vector represents the word when it is added to a context vector
 -}
 
getNHashes n word =
    [h seed | seed <- [1..n]]
    where h seed = fromIntegral $ Murmur.asWord64 $ Murmur.hash64WithSeed seed word
hashWord word =
    V.modify (vimap (\i _ -> evenOddToPosNeg $ fromIntegral i) indices) newEmptyHash
    where
        evenOddToPosNeg x = (mod x 2) * 2 - 1
        indices = map (`mod` context_dims) $ getNHashes word_dims word

-- Join hash-vectors of the words in the paragraph
hashChunk tokens =
    foldl' hashAnotherWord newEmptyHash tokens
    where
        hashAnotherWord vec word = V.zipWith (+) vec (hashWord word)

newEmptyHash = V.replicate context_dims context_type



-- -- Bloom
    
{-
 - Simple counting bloom filter
 - It will add a word to a filter until it reaches the bloom_threshold
 - Then it will no longer add (to avoid overflow) but will return popular=True
 -}
bloom_hash_count = 10
bloom_bin_count = 25000000
bloom_threshold = 3

-- Count a word, Return (is_popular, new_bloom)
addToBloom :: (PrimMonad m) => VM.MVector (PrimState m) Word8 -> String -> m Bool
--addToBloom :: V.Vector Word8 -> String -> (Bool, V.Vector Word8)
addToBloom bloom word = do
    counts <- sequence [VM.read bloom i | i <- indices ]
    if minimum counts >= bloom_threshold
       then return True
       else do
           vmap (incrementClamp) indices bloom
           return False
    where
        incrementClamp a = max a (a+1)
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]

makeNewBloom :: PrimMonad m => m (VM.MVector (PrimState m) Word8)
makeNewBloom =
    VM.replicate bloom_bin_count (0::Word8)
    

-- -- Bloomed Map
data BloomMap m = BloomMap (VM.MVector (PrimState m) Word8) (M.Map String (V.Vector Int64))

makeEmptyBloomMap :: PrimMonad m => m (BloomMap m)
makeEmptyBloomMap = do
    bloom <- makeNewBloom
    return $ BloomMap bloom M.empty



-- -- Random Indexing

-- If the word appears in the bloom at least n times, add context vectors
addWordContext :: (PrimMonad m) => V.Vector Int64 -> BloomMap m -> String -> m (BloomMap m)
addWordContext new_context (BloomMap bloom context_map) word = do
    popular <- addToBloom bloom word
    if popular
       then return $ BloomMap bloom context_map
       else return $ BloomMap bloom (M.insertWith (V.zipWith (+)) word new_context context_map)
--    | popular = ((BloomMap new_bloom new_context_map), new_context)
--    | otherwise = ((BloomMap new_bloom context_map), new_context)
--    where
--        (popular, new_bloom) = addToBloom bloom word
--        new_context_map = M.insertWith (V.zipWith (+)) word new_context context_map

-- Chain addWordContext, but only calculate paragraph context once (used by binaryChunkContext)
addBinaryMultiwordContext :: (PrimMonad m) => BloomMap m -> [String] -> m (BloomMap m)
addBinaryMultiwordContext bloom_map tokens =
    foldM (addWordContext chunk_context) bloom_map tokens
    where
        chunk_context = hashChunk tokens

-- Fill contexts based on whether words cooccur in a chunk
indexBinaryChunks :: PrimMonad m => [[String]] -> m (BloomMap m)
indexBinaryChunks tokenized_chunks = do
    bmap <- makeEmptyBloomMap
    foldM (addBinaryMultiwordContext) bmap tokenized_chunks