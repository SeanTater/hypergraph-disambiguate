{-
    You'll need:
        murmur-hash
        vector
-}
import qualified RIndex.RIndex as RIndex

import System.IO
import System.Environment
import qualified Data.Map.Strict as M
import Data.List
import Database.HDBC
import Database.HDBC.Sqlite3



data WordCount = WordCount String Int deriving (Show)
    

main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    wordcounts <- quickQuery conn "SELECT word, para_id FROM word_counts LIMIT 1000" []
    
    let wc_as_wc = [ WordCount (fromSql w) (fromSql p) | (w : p : _) <- wordcounts ]
    let paragraphs = groupBy (\(WordCount w1 p1) (WordCount w2 p2)  -> p1 == p2) wc_as_wc
    let paragraph_tokens = [ [ w | WordCount w _  <- paragraph] | paragraph <- paragraphs]
    
    let (RIndex.BloomMap _ bmap) = RIndex.indexBinaryChunks paragraph_tokens
    print $ M.keysSet bmap
    disconnect conn

{-
main = do
    line <- getLine
    print $ RIndex.indexBinaryChunks $ [ words line ]
-}