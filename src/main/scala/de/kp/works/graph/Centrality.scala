package de.kp.works.graph
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

class Centrality {
  /**
   * Degree centrality assigns an importance score based simply on the
   * number of (in- and outgoing) links held by each node.
   *
   * This specifies how many direct, ‘one hop’ connections each node has
   * to other nodes in the network.
   *
   * It is made to find very connected individuals, popular individuals,
   * individuals who are likely to hold most information or individuals who
   * can quickly connect with the wider network.
   */
  def degree():Unit = {
    throw new Exception("Not implemented yet.")
  }

  /**
   * Betweenness centrality measures the number of times a node lies on the
   * shortest path between other nodes.
   *
   * This measure shows which nodes are ‘bridges’ between nodes in a network.
   * It does this by identifying all the shortest paths and then counting how
   * many times each node falls on one.
   *
   * It is made to find the individuals who influence the flow around a system.
   *
   * Betweenness is useful for analyzing communication dynamics, but should be
   * used with care. A high betweenness count could indicate someone holds authority
   * over disparate clusters in a network, or just that they are on the periphery of
   * both clusters.
   */
  def betweenness():Unit = {
    throw new Exception("Not implemented yet.")
  }

  /**
   * Closeness centrality scores each node based on their ‘closeness’ to all other
   * nodes in the network.
   *
   * This measure calculates the shortest paths between all nodes, then assigns each
   * node a score based on its sum of shortest paths.
   *
   * Its made to find the individuals who are best placed to influence the entire network
   * most quickly.
   *
   * Closeness centrality can help find good ‘broadcasters’, but in a highly-connected
   * network, you will often find all nodes have a similar score. What may be more useful
   * is using Closeness to find influencers in a single cluster.
   */
  def closeness():Unit = {
    throw new Exception("Not implemented yet.")
  }

  /**
   * Like degree centrality, EigenCentrality measures a node’s influence based on the number
   * of links it has to other nodes in the network. EigenCentrality then goes a step further
   * by also taking into account how well connected a node is, and how many links their connections
   * have, and so on through the network.
   *
   * By calculating the extended connections of a node, EigenCentrality can identify nodes with
   * influence over the whole network, not just those directly connected to it.
   *
   * EigenCentrality is a good ‘all-round’ SNA score, handy for understanding human social networks,
   * but also for understanding networks like malware propagation.
   *
   * This method calculate each node’s EigenCentrality by converging on an eigenvector using the
   * power iteration method.
   */
  def eigen():Unit = {
    throw new Exception("Not implemented yet.")
  }

  /**
   * PageRank is a variant of EigenCentrality, also assigning nodes a score based on their connections,
   * and their connections’ connections. The difference is that PageRank also takes link direction and
   * weight into account – so links can only pass influence in one direction, and pass different amounts
   * of influence.
   *
   * This measure uncovers nodes whose influence extends beyond their direct connections into the wider
   * network.
   *
   * Because it takes into account direction and connection weight, PageRank can be helpful for understanding
   * citations and authority.
   *
   * PageRank is famously one of the ranking algorithms behind the original Google search engine (the ‘Page’
   * part of its name comes from creator and Google founder, Sergei Brin).
   */
  def pageRank():Unit = {
    throw new Exception("Not implemented yet.")
  }
}
