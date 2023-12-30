/*
 * Copyright 2019 Jack Henry & Associates, Inc.®
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.banno.kafka

import munit.CatsEffectSuite

class ProcessingAndCommittingSpec extends CatsEffectSuite with KafkaSpec {

  test("processingAndCommitting commits after number of records") {
    
    /*
    assert no committed offsets
    process record
    assert no committed offsets
    process record
    assert no committed offsets
    ...
    process record
    assert offsets got committed
     */
  }

  test("processingAndCommitting commits after elapsed time") {
    /*
    assert no committed offsets
    process records
    ...
    after elapsed time
    assert offsets got committed
     */
    
  }

  test("processingAndCommitting does not commit after failure") {
    
  }

}
