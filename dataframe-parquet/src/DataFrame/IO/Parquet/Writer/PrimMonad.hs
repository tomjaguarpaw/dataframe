{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.IO.Parquet.Writer.PrimMonad (
    PrimM,
    runPrimM,
) where

import Bluefin.Compound (
    Generic,
    Handle,
    OneWayCoercible (..),
    OneWayCoercibleHandle (..),
    gOneWayCoercible,
    mapHandle,
 )
import Bluefin.DslBuilder (DslBuilder, dslBuilder, runDslBuilder)
import Bluefin.Eff (Eff, type (<:))
import Bluefin.IO (IOE, effIO)
import qualified Bluefin.Prim as P
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Primitive (PrimMonad (..))

{- | A monad with Bluefin's primitive-state capability and an explicit IO
capability. This lets the writer keep its current interleaving of mutable
buffer operations and file writes.
-}
data IOAndPrim e1 e2 = MkIOAndPrim (IOE e2) (P.Prim e1 e2)
    deriving (Handle) via OneWayCoercibleHandle (IOAndPrim e1)
    deriving stock (Generic)

instance (e2 <: es) => OneWayCoercible (IOAndPrim e1 e2) (IOAndPrim e1 es) where
    oneWayCoercibleImpl = gOneWayCoercible

newtype PrimM e1 a = MkPrimM (DslBuilder (IOAndPrim e1) a)
    deriving newtype (Functor, Applicative, Monad)

instance PrimMonad (PrimM e1) where
    type PrimState (PrimM e1) = P.PrimStateEff e1
    primitive f =
        MkPrimM (dslBuilder (\(MkIOAndPrim _ prim) -> P.primitive prim f))

instance MonadIO (PrimM e1) where
    liftIO action =
        MkPrimM (dslBuilder (\(MkIOAndPrim ioe _) -> effIO ioe action))

{- | Run a primitive computation using Bluefin's scoped primitive-state and
IO capabilities.
-}
runPrimM ::
    (e1 <: es, e2 <: es) =>
    IOE e1 ->
    P.Prim e3 e2 ->
    PrimM e3 a ->
    Eff es a
runPrimM ioe prim (MkPrimM builder) =
    runDslBuilder (MkIOAndPrim (mapHandle ioe) (mapHandle prim)) builder
