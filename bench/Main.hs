{-# language TypeApplications #-}
{-# language BangPatterns #-}

import Data.Char (ord)
import Data.Word (Word8,Word64)
import Data.Primitive (ByteArray)
import Database.Influx.LineProtocol
import Data.ByteArray.Builder (run)
import Data.Vector (Vector)
import Gauge.Main
import Gauge.Main.Options

import qualified Data.Primitive as PM
import qualified GHC.Exts as Exts
import qualified Data.Bytes.Chunks as Chunks

main :: IO ()
main = do
  let sz = 512 + PM.sizeofByteArray (Chunks.concat (foldMap (run 1 . encodePoint) points))
  defaultMainWith (defaultConfig{forceGC=False})
    [ -- In the static benchmark, field values have likely
      -- been floated to the top level. Consequently,
      -- improvements or regressions affecting field-value
      -- escaping are not reflected.
      bench "static" (whnf (\x -> run sz (foldMap encodePoint x)) points)
    ]
    
points :: Vector Point
points = Exts.fromList
  [ Point
    { measurement = rocks
    , tags = color (tagValueByteArray (str "green"))
    , fields = temperature (fieldValueWord64 97)
    , time = 1442365427643
    }
  , Point
    { measurement = weird
    , tags = color (tagValueByteArray (str "green"))
    , fields = egressIngress (fieldValueWord64 246132155) (fieldValueWord64 46254355)
    , time = 154236542324
    }
  , Point
    { measurement = symbols
    , tags = color (tagValueByteArray (str "black"))
    , fields = temperature (fieldValueWord64 104)
    , time = 1442365427643
    }
  , Point
    { measurement = rocks
    , tags = mempty
    , fields = ageName (fieldValueWord64 57) (fieldValueByteArray (str "Randy the Great"))
    , time = 1342365427647
    }
  , Point
    { measurement = rocks
    , tags = mempty
    , fields = name
        (fieldValueByteArray (str "Bud \"The Gun\" Peter\\son"))
    , time = 0
    }
  , Point
    { measurement = rocks
    , tags = color (tagValueByteArray (str "red"))
    , fields = egressIngress (fieldValueWord64 246) (fieldValueWord64 4361)
    , time = (maxBound :: Word64)
    }
  ]

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

