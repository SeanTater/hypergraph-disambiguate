{-
    You'll need:
        murmur-hash
        vector
-}
{-# LANGUAGE BangPatterns #-}
import qualified RIndex.RIndex as RIndex

import System.IO ( hSetBuffering, BufferMode(NoBuffering), stdout, print)
import Control.Monad
import qualified System.ProgressBar as PB
import System.Environment
import Data.ByteString (pack)
import Data.ByteString.Lazy.Builder (int32LE, toLazyByteString)
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as V
import Data.List
import Data.Monoid
import Data.Foldable (foldMap)
import Database.HDBC
import Database.HDBC.Sqlite3

data WordCount = WordCount String Int deriving (Show)
    
progress (si:_) =
    when (i `mod` 1000 == 0) $
        PB.progressBar (PB.msg "Indexing") (\x y -> PB.exact x y ++ PB.percentage x y) 80 i 1264293473
    where i = fromSql si


processBlock conn bmap start = do
    wordcounts <- quickQuery' conn "SELECT rowid, word, para_id FROM word_counts WHERE rowid > ? LIMIT 100000" [toSql start]
    if null wordcounts || start > 100000000
        then    return bmap
        else do
            let paragraphs = groupBy (\(i1 : w1 : p1 : _) (i2 : w2 : p2 : _)  -> (fromSql p1 :: Int) == fromSql p2) wordcounts
                lastrowid = fromSql $ (\(i:_) -> i) $ last wordcounts
            
            -- handle paragraphs
            newbmap <- foldM' processParagraph bmap paragraphs
            let RIndex.BloomMap _ mp = newbmap
            putStr "Unique words indexed: "
            print $ M.size mp
            processBlock conn newbmap lastrowid
        
    where  
        processParagraph bmap paragraph =
            RIndex.addBinaryMultiwordContext bmap [ fromSql w :: String | ( _ : w : _ )  <- paragraph]
    
foldM' :: (Monad m) => (a -> b -> m a) -> a -> [b] -> m a
foldM' _ z [] = return z
foldM' f z (x:xs) = do
  z' <- f z x
  z' `seq` foldM' f z' xs
    

main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    
    empty_bmap <- RIndex.makeEmptyBloomMap
    RIndex.BloomMap _ mp <- processBlock conn empty_bmap (-1::Int)
    
    putStr "\nKey count: "
    print $ M.size mp
    --print $ length wc_as_wc
    
    committer <- prepare conn "INSERT OR REPLACE INTO rindex (word, context) VALUES (?, ?);"
    executeMany committer [ [toSql word, toSql $ toLazyByteString $ foldMap int32LE $ V.toList vec] | (word, vec) <- M.toList mp]
    commit conn
    disconnect conn