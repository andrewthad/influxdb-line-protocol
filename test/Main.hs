{-# language TypeApplications #-}

import Data.Char (ord)
import Data.Word
import Data.Primitive (ByteArray)
import Database.Influx.LineProtocol
import Data.ByteArray.Builder (run)
import System.IO
import Control.Monad (when)

import qualified GHC.Exts as Exts
import qualified Data.Bytes.Chunks as Chunks

main :: IO ()
main = do
  putStrLn "Running Test A"
  when (expectedA /= actualA) $ do
    hPutStrLn stderr "Test A failed"
    hPutStrLn stderr ("Expected: " ++ show expectedA)
    hPutStrLn stderr ("Actually: " ++ show actualA)
    fail ""
  putStrLn "Running Test B"
  when (expectedB /= actualB) $ do
    hPutStrLn stderr "Test B failed"
    hPutStrLn stderr ("Expected: " ++ show expectedB)
    hPutStrLn stderr ("Actually: " ++ show actualB)
    fail ""
  putStrLn "Running Test C"
  when (expectedC /= actualC) $ do
    hPutStrLn stderr "Test C failed"
    hPutStrLn stderr ("Expected: " ++ show expectedC)
    hPutStrLn stderr ("Actually: " ++ show actualC)
    fail ""
  putStrLn "Running Test D"
  when (expectedD /= actualD) $ do
    hPutStrLn stderr "Test D failed"
    hPutStrLn stderr ("Expected: " ++ show expectedD)
    hPutStrLn stderr ("Actually: " ++ show actualD)
    fail ""
  putStrLn "Running Test E"
  when (expectedE /= actualE) $ do
    hPutStrLn stderr "Test E failed"
    hPutStrLn stderr ("Expected: " ++ show expectedE)
    hPutStrLn stderr ("Actually: " ++ show actualE)
    fail ""
  putStrLn "Running Test F"
  when (expectedF /= actualF) $ do
    hPutStrLn stderr "Test F failed"
    hPutStrLn stderr ("Expected: " ++ show expectedF)
    hPutStrLn stderr ("Actually: " ++ show actualF)
    fail ""
  putStrLn "All tests passed"
    
expectedA :: ByteArray
expectedA = str
  "rocks,color=green temperature=97 1442365427643\n"

expectedB :: ByteArray
expectedB = str
  "my\\,strange\\,\\ measurement\\ name,color=green egress=246132155,ingress=46254355 1442365427643\n"

expectedC :: ByteArray
expectedC = str
  "\\ \\,\\ \\,\\ ,color=green temperature=104 1442365427643\n"

expectedD :: ByteArray
expectedD = str
  "rocks age=57,name=\"Randy the Great\" 1342365427647\n"

expectedE :: ByteArray
expectedE = str
  "rocks name=\"Bud \\\"The Gun\\\" Peter\\\\son\" 0\n"

expectedF :: ByteArray
expectedF = str
  "rocks,color=red egress=246,ingress=4361 18446744073709551615\n"

actualA :: ByteArray
actualA = Chunks.concatU $ run 7 $ encodePoint
  ( Point
    { measurement = rocks
    , tags = color (tagValueByteArray (str "green"))
    , fields = temperature (fieldValueWord64 97)
    , time = 1442365427643
    }
  )

actualB :: ByteArray
actualB = Chunks.concatU $ run 7 $ encodePoint
  ( Point
    { measurement = weird
    , tags = color (tagValueByteArray (str "green"))
    , fields = egressIngress (fieldValueWord64 246132155) (fieldValueWord64 46254355)
    , time = 1442365427643
    }
  ) 

actualC :: ByteArray
actualC = Chunks.concatU $ run 7 $ encodePoint
  ( Point
    { measurement = symbols
    , tags = color (tagValueByteArray (str "green"))
    , fields = temperature (fieldValueWord64 104)
    , time = 1442365427643
    }
  ) 

actualD :: ByteArray
actualD = Chunks.concatU $ run 23 $ encodePoint
  ( Point
    { measurement = rocks
    , tags = mempty
    , fields = ageName (fieldValueWord64 57) (fieldValueByteArray (str "Randy the Great"))
    , time = 1342365427647
    }
  ) 

actualE :: ByteArray
actualE = Chunks.concatU $ run 23 $ encodePoint
  ( Point
    { measurement = rocks
    , tags = mempty
    , fields = name
        (fieldValueByteArray (str "Bud \"The Gun\" Peter\\son"))
    , time = 0
    }
  ) 

actualF :: ByteArray
actualF = Chunks.concatU $ run 7 $ encodePoint
  ( Point
    { measurement = rocks
    , tags = color (tagValueByteArray (str "red"))
    , fields = egressIngress (fieldValueWord64 246) (fieldValueWord64 4361)
    , time = (maxBound :: Word64)
    }
  ) 

rocks :: Measurement
rocks = measurementByteArray (str "rocks")
  
weird :: Measurement
weird = measurementByteArray (str "my,strange, measurement name")
  
symbols :: Measurement
symbols = measurementByteArray (str " , , ")
  
color :: TagValue -> Tags
color = tags1 (tagKeyByteArray (str "color"))

temperature :: FieldValue -> Fields
temperature = fields1 (fieldKeyByteArray (str "temperature"))

egressIngress :: FieldValue -> FieldValue -> Fields
egressIngress = fields2
  (fieldKeyByteArray (str "egress"))
  (fieldKeyByteArray (str "ingress"))

ageName :: FieldValue -> FieldValue -> Fields
ageName = fields2
  (fieldKeyByteArray (str "age"))
  (fieldKeyByteArray (str "name"))

name :: FieldValue -> Fields
name = fields1 (fieldKeyByteArray (str "name"))

str :: String -> ByteArray
str = Exts.fromList . map (fromIntegral @Int @Word8 . ord)
