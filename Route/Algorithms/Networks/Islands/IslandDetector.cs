﻿/*
 *  Licensed to SharpSoftware under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  SharpSoftware licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using Reminiscence.Arrays;
using Route.Algorithms.Collections;
using Route.Data.Edges;
using Route.Profiles;

namespace Route.Algorithms.Networks.Islands
{
	/// <summary>
	/// An island detector based on a set of profiles.
	/// </summary>
	public class IslandDetector : AlgorithmBase
	{
		private readonly Func<ushort, Factor>[] _profiles;
        private readonly ushort[] _islands; // holds the island ID per vertex.
        private readonly Dictionary<ushort, long> _islandSizes; // holds the size per island.
        private readonly RouterDb _routerDb;
        private const long NO_DATA = long.MaxValue;
        private const long NO_ISLAND = long.MaxValue - 1;

        /// <summary>
        /// A value representing a restricted vertex, could be part of multiple islands.
        /// </summary>
        public const ushort RESTRICTED = ushort.MaxValue - 2;
        private const long RESTRICTED_FULL = long.MaxValue - 2;

        private readonly Restrictions.RestrictionCollection _restrictionCollection;

        /// <summary>
        /// A value representing a singleton island.
        /// </summary>
        public const ushort SINGLETON_ISLAND = ushort.MaxValue;
        private const long SINGLETON_ISLAND_FULL = long.MaxValue;
        
        private const ushort OVERFLOW_ISLAND = ushort.MaxValue - 1; // A value representing an island when there is overflow.

        /// <summary>
        /// Creates a new island detector.
        /// </summary>
		public IslandDetector(RouterDb routerDb, Func<ushort, Factor>[] profiles, Restrictions.RestrictionCollection restrictionCollection = null)
		{
            _profiles = profiles;
            _routerDb = routerDb;
            _restrictionCollection = restrictionCollection;

            _islands = new ushort[_routerDb.Network.VertexCount];
            _islandSizes = new Dictionary<ushort, long>();
		}

        private long[] _fullIslands;
        private Dictionary<long, long> _fullIslandSizes;
        private ArrayBase<long> _index;
        private Collections.Stack<long> _stack;
        private SparseLongIndex _onStack;

        private long _nextIndex = 0;
        private long _nextIsland = 0;

        /// <summary>
        /// Runs the island detection.
        /// </summary>
		protected override void DoRun(CancellationToken cancellationToken)
        {
            _fullIslands = new long[_islands.Length];
            _fullIslandSizes = new Dictionary<long, long>();
            
            _onStack = new SparseLongIndex();
            var vertexCount = _routerDb.Network.GeometricGraph.Graph.VertexCount;

            // initialize all islands to NO_ISLAND.
            for (long i = 0; i < _islands.Length; i++)
            {
                _fullIslands[i] = NO_ISLAND;

                if (_restrictionCollection != null)
                {
                    _restrictionCollection.Update(i);

                    for (var r = 0; r < _restrictionCollection.Count; r++)
                    {
                        var restriction = _restrictionCollection[r];

                        if (restriction.Vertex2 == Constants.NO_VERTEX &&
                            restriction.Vertex3 == Constants.NO_VERTEX)
                        {
                            _fullIslands[i] = RESTRICTED_FULL;
                            break;
                        }
                        else
                        {
                            // TODO: support other restrictions.
                        }
                    }
                }
            }

            // build index data structure and stack.
            _index = Context.ArrayFactory.CreateMemoryBackedArray<long>(vertexCount * 2);
            for (var i = 0; i < _index.Length; i++)
            {
                _index[i] = NO_DATA;
            }
            _stack = new Collections.Stack<long>();

            // https://en.wikipedia.org/wiki/Tarjan's_strongly_connected_components_algorithm
            for (long v = 0; v < vertexCount; v++)
            {
                var vIndex = _index[v * 2];
                if (vIndex != NO_DATA)
                {
                    continue;
                }

                StrongConnect(v);
            }
            
            // sort islands.
            var sortedIslands = new List<KeyValuePair<long, long>>(_fullIslandSizes);
            sortedIslands.Sort((x, y) => -x.Value.CompareTo(y.Value));
            var newIds = new Dictionary<long, ushort>();
            for (long i = 0; i < sortedIslands.Count; i++)
            {
                var id = i;
                if (id > OVERFLOW_ISLAND) id = OVERFLOW_ISLAND;
                newIds[sortedIslands[(int)i].Key] = (ushort)id;
            }
            for (var v = 0; v < _fullIslands.Length; v++)
            {
                if (newIds.TryGetValue(_fullIslands[v], out var newId))
                {
                    _islands[v] = newId;
                }
                else if (_fullIslands[v] == SINGLETON_ISLAND_FULL)
                {
                    _islands[v] = SINGLETON_ISLAND;
                }else if (_fullIslands[v] == RESTRICTED_FULL)
                {
                    _islands[v] = RESTRICTED;
                }
            }
            _islandSizes.Clear();
            foreach (var sortedIsland in sortedIslands)
            {
                if (newIds.TryGetValue(sortedIsland.Key, out var newId))
                {
                    _islandSizes[newId] = sortedIsland.Value;
                }
            }
        }

        private void StrongConnect(long v)
        {
            var nextStack = new Collections.Stack<long>();
            nextStack.Push(Constants.NO_VERTEX);
            nextStack.Push(v);

            while (nextStack.Count > 0)
            {
                v = nextStack.Pop();
                var parent = nextStack.Pop();

                if (_fullIslands[v] != NO_ISLAND)
                {
                    continue;
                }
                
                // 2 options: 
                // OPTION 1: vertex was already processed, check if it's a root vertex.
                if (_index[v * 2 + 0] != NO_DATA)
                { // vertex was already processed, do wrap-up.
                    if (parent != Constants.NO_VERTEX)
                    {
                        var vLowLink = _index[v * 2 + 1];
                        if (vLowLink < _index[parent * 2 + 1])
                        {
                            _index[parent * 2 + 1] = vLowLink;
                        }
                    }

                    if (_index[v * 2 + 0] == _index[v * 2 + 1])
                    { // this was a root node so this is an island!
                      // pop from stack until root reached.
                        var island = _nextIsland;
                        _nextIsland++;

                        long size = 0;
                        long islandVertex = Constants.NO_VERTEX;
                        do
                        {
                            islandVertex = _stack.Pop();
                            _onStack.Remove(islandVertex);

                            size++;
                            _fullIslands[islandVertex] = island;
                        } while (islandVertex != v);

                        if (size == 1)
                        { // only the root vertex, meaning this is a singleton.
                            _fullIslands[v] = SINGLETON_ISLAND_FULL;
                            _nextIsland--; // reset island counter.
                        }
                        else
                        { // keep island size.
                            _fullIslandSizes[island] = size;
                        }
                    }

                    continue;
                }

                // OPTION 2: vertex wasn't already processed, process it and queue it's neigbours.
                // push again to trigger OPTION1.
                nextStack.Push(parent);
                nextStack.Push(v);

                var enumerator = _routerDb.Network.GeometricGraph.Graph.GetEdgeEnumerator();
                enumerator.MoveTo(v);

                _index[v * 2 + 0] = _nextIndex;
                _index[v * 2 + 1] = _nextIndex;
                _nextIndex++;

                _stack.Push(v);
                _onStack.Add(v);

                if (enumerator.MoveTo(v))
                {
                    while (enumerator.MoveNext())
                    {
                        EdgeDataSerializer.Deserialize(enumerator.Data0, out _, out var edgeProfile);

                        var access = this.GetAccess(edgeProfile);

                        if (enumerator.DataInverted)
                        {
                            if (access == Access.OnewayBackward)
                            {
                                access = Access.OnewayForward;
                            }
                            else if (access == Access.OnewayForward)
                            {
                                access = Access.OnewayBackward;
                            }
                        }

                        if (access != Access.OnewayForward &&
                            access != Access.Bidirectional)
                        {
                            continue;
                        }

                        var n = enumerator.To;
                        
                        if (_islands[n] == RESTRICTED)
                        { // check if this neighbour is restricted, if so ignore.
                            continue;
                        }

                        var nIndex = _index[n * 2 + 0];
                        if (nIndex == NO_DATA)
                        { // queue parent and neighbour.
                            nextStack.Push(v);
                            nextStack.Push(n);
                        }
                        else if (_onStack.Contains(n))
                        {
                            if (nIndex < _index[v * 2 + 1])
                            {
                                _index[v * 2 + 1] = nIndex;
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the islands.
        /// </summary>
        public ushort[] Islands
        {
            get
            {
                return _islands;
            }
        }

        /// <summary>
        /// Gets the island sizes.
        /// </summary>
        public Dictionary<ushort, long> IslandSizes
        {
            get
            {
                return _islandSizes;
            }
        }
        
        /// <summary>
        /// Returns true if the edge profile can be traversed by any of the profiles and at least on of the profiles notifies as oneway.
        /// </summary>
        private Access GetAccess(ushort edgeProfile)
        {
            var access = Access.None;
            for (var p = 0; p < _profiles.Length; p++)
            {
                var f = _profiles[p](edgeProfile);
                if (f.Value != 0)
                {
                    if (f.Direction == 0)
                    {
                        if (access == Access.None)
                        {
                            access = Access.Bidirectional;
                        }
                    }
                    else if (f.Direction == 1)
                    {
                        if (access == Access.OnewayBackward)
                        {
                            return Access.None;
                        }
                        access = Access.OnewayForward;
                    }
                    else if (f.Direction == 2)
                    {
                        if (access == Access.OnewayForward)
                        {
                            return Access.None;
                        }
                        access = Access.OnewayBackward;
                    }
                }
            }
            return access;
        }

        private enum Access
        {
            None,
            OnewayForward,
            OnewayBackward,
            Bidirectional
        }
    }
}
