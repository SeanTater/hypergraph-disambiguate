{-
    You'll need:
        murmur-hash
        vector
-}
import qualified RIndex.RIndex as RIndex

main = do
    line <- getLine
    print $ RIndex.indexBinaryChunks $ [ words line ]