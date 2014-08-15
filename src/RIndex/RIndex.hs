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

import Control.Monad.ST
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.Map.Strict as M
import qualified Data.Digest.Murmur64 as Murmur

-- -- Utility

-- Use modify to get around state

--vmap :: (VM.Unbox a) => (a -> a) -> [Int] -> VM.MVector s a -> ST s ()
vmap func indices ivec =
    vimap (\_ b -> func b) indices ivec

--vimap :: (VM.Unbox a) => (Int -> a -> a) -> [Int] -> VM.MVector s a -> ST s ()
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
addToBloom :: V.Vector Word8 -> String -> (Bool, V.Vector Word8)
addToBloom bloom word =
    (popular, if popular then bloom else V.modify (vmap (incrementClamp) indices) bloom)
    where
        incrementClamp a = max a (a+1)
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]
        min_count = minimum [ (V.!) bloom idx | idx <- indices ]
        popular = min_count >= bloom_threshold

makeNewBloom = 
    V.thaw $ V.replicate bloom_bin_count (0::Word8)


-- -- Bloomed Map
data BloomMap = BloomMap (VM.IOVector Word8) (M.Map String (V.Vector Int64))

makeEmptyBloomMap = BloomMap makeNewBloom M.empty




-- -- Random Indexing

-- If the word appears in the bloom at least n times, add context vectors
addWordContext :: (BloomMap, V.Vector Int64) -> String -> (BloomMap, V.Vector Int64)
addWordContext ((BloomMap bloom context_map), new_context) word
    | popular = ((BloomMap new_bloom new_context_map), new_context)
    | otherwise = ((BloomMap new_bloom context_map), new_context)
    where
        (popular, new_bloom) = addToBloom bloom word
        new_context_map = M.insertWith (V.zipWith (+)) word new_context context_map

-- Chain addWordContext, but only calculate paragraph context once (used by binaryChunkContext)
addBinaryMultiwordContext bloom_map tokens =
    new_bloom_map
    where
        chunk_context = hashChunk tokens
        (new_bloom_map, _) = foldl (addWordContext) (bloom_map, chunk_context) tokens

-- Fill contexts based on whether words cooccur in a chunk
indexBinaryChunks tokenized_chunks =
    foldl' addBinaryMultiwordContext makeEmptyBloomMap tokenized_chunks