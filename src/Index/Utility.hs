module Index.Utility where
import qualified Data.Digest.Murmur64 as Murmur
import qualified Data.Text.Encoding as TextEncoding
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.HashSet as HS
import qualified Data.Text as Text
--import qualified Data.List.Stream as LS
import Control.Monad.Primitive
import Data.Word

{-| Get murmur-based hashes of a string.
Multiple hashes are made by replacing the seed value with
integers from 1 to n.
-}
getNHashes :: Word64 -> Text.Text -> [Word64]
getNHashes n word =
    [h seed | seed <- [1..n]]
    where h seed = Murmur.asWord64 $ Murmur.hash64WithSeed seed $ TextEncoding.encodeUtf8 word

-- -- Utility
-- | Apply a pure function (with index) to a list of indices on a monadic vector.
applyMV :: (PrimMonad m, V.Unbox t) => (Int -> t -> t) -> VM.MVector (PrimState m) t -> Int -> m ()
applyMV func vec i = do
    e_in <- VM.read vec i
    VM.write vec i (func i e_in)
    return ()
    
--conservativeTokenize :: Text.Text -> [Text.Text]
conservativeTokenize = 
    filter (not . Text.null) . Text.split spaceOrPunctuation . Text.toLower 
    where
        spaceOrPunctuation char = HS.member char delimiters 
        delimiters = HS.fromList "\t ~`!@#$%^&*()_+-={}|[]\\:\";'<>?,./"