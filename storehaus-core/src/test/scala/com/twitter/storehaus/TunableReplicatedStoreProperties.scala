/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.storehaus

import org.scalacheck.Properties

object TunableReplicatedStoreProperties extends Properties("TunableReplicatedStore") {
  import StoreProperties.storeTest

  val REPLICAS = 10

  property("TunableReplicatedStore, readConsistency=One, writeConsistency=One") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.One, ConsistencyLevel.One)
    }
  }

  property("TunableReplicatedStore, readConsistency=One, writeConsistency=Quorum") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.One, ConsistencyLevel.Quorum)
    }
  }

  property("TunableReplicatedStore, readConsistency=One, writeConsistency=All") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.One, ConsistencyLevel.All)
    }
  }

  property("TunableReplicatedStore, readConsistency=Quorum, writeConsistency=One") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.Quorum, ConsistencyLevel.One)
    }
  }

  property("TunableReplicatedStore, readConsistency=Quorum, writeConsistency=Quorum") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.Quorum, ConsistencyLevel.Quorum)
    }
  }

  property("TunableReplicatedStore, readConsistency=Quorum, writeConsistency=All") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.Quorum, ConsistencyLevel.All)
    }
  }

  property("TunableReplicatedStore, readConsistency=All, writeConsistency=One") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.All, ConsistencyLevel.One)
    }
  }

  property("TunableReplicatedStore, readConsistency=All, writeConsistency=Quorum") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.All, ConsistencyLevel.Quorum)
    }
  }

  property("TunableReplicatedStore, readConsistency=All, writeConsistency=All") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.All, ConsistencyLevel.All)
    }
  }

  property("TunableReplicatedStore, readConsistency=Quorum, writeConsistency=Quorum, readRepair=true, writeRollback=true") = {
    storeTest {
      TunableReplicatedStore.fromSeq((0 until REPLICAS).map { _ => new JMapStore[Int, String] },
        ConsistencyLevel.All, ConsistencyLevel.All, true, true)
    }
  }

}

