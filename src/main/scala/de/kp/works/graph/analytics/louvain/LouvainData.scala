package de.kp.works.graph.analytics.louvain
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

/**
 * Louvain vertex state, contains all information
 * needed for louvain community detection.
 */
class LouvainData(
    var name: String,
    var community: Long,
    var communityName: String,
    var communitySigmaTot: Long,
    var internalWeight: Long,
    var nodeWeight: Long, var changed: Boolean) extends Serializable {

  def this() = this(null, -1L, null, 0L, 0L, 0L, false)

}
