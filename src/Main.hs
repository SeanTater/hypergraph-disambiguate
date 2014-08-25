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
import Data.ByteString.Lazy.Builder (int32LE, toLazyByteString)
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as V
import Data.List
import qualified Data.Set as S
import Data.Monoid
import Data.Foldable -- (foldMap)
import Data.Binary
import Database.HDBC
import Database.HDBC.Sqlite3

data WordCount = WordCount String Int deriving (Show)

indexBlock conn bmap start = do
    rowid_paragraphs <- quickQuery' conn "SELECT rowid, content FROM paragraphs WHERE rowid > ? LIMIT 1000" [toSql start]
    
    if null rowid_paragraphs
        then    return bmap
        else do
            let paragraphs = map (fromSql . last) rowid_paragraphs
                lastrowid = fromSql $ head $ last rowid_paragraphs
            
            -- handle paragraphs
            newbmap <- foldlM processParagraph bmap paragraphs
            let RIndex.BloomMap _ mp = newbmap
            putStr "Unique words indexed: "
            print $ M.size mp
            indexBlock conn newbmap lastrowid
        
    where
        processParagraph bmap paragraph =
            RIndex.addBinaryMultiwordContext bmap $ uniqueWords paragraph
        uniqueWords = S.toList . S.fromList . words -- TODO: Do better splitting


main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    
    empty_bmap <- RIndex.makeEmptyBloomMap
    RIndex.BloomMap _ mp <- indexBlock conn empty_bmap (0::Int)
    
    putStr "\nKey count: "
    print $ M.size mp
    
    committer <- prepare conn "INSERT OR REPLACE INTO rindex (word, context) VALUES (?, ?);"
    executeMany committer [ [toSql word, toSql $ encode vec] | (word, vec) <- M.toList mp]
    commit conn
    disconnect conn