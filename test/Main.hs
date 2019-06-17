{-# language TypeApplications #-}

import Data.Char (ord)
import Data.Word
import Data.Primitive (ByteArray)
import Database.Influx.LineProtocol
import Data.ByteArray.Builder.Small (run)
import System.IO
import Control.Monad (when)

import qualified GHC.Exts as Exts

main :: IO ()
main = do
  when (expectedA /= actualA) $ do
    hPutStrLn stderr "Test A failed"
    hPutStrLn stderr ("Expected: " ++ show expectedA)
    hPutStrLn stderr ("Actually: " ++ show actualA)
    fail ""
  putStrLn "All tests passed"
    
expectedA :: ByteArray
expectedA = str
  "rocks,color=green temperature=97 1442365427643\n"

actualA :: ByteArray
actualA = run 7 $ encodePoint
  ( Point
    { measurement = rocks
    , tags = color (tagValue (str "green"))
    , fields = temperature (fieldValueWord64 97)
    , time = 1442365427643
    }
  ) 

rocks :: Measurement
rocks = escapeMeasurement (str "rocks")
  
color :: TagValue -> Tags
color = tags1 (tagKey (str "color"))

temperature :: FieldValue -> Fields
temperature = fields1 (fieldKey (str "temperature"))

str :: String -> ByteArray
str = Exts.fromList . map (fromIntegral @Int @Word8 . ord)
