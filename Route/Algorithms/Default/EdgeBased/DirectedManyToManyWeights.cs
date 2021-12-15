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

using System.Threading;
using Route.Algorithms.Restrictions;
using Route.Algorithms.Weights;
using Route.Graphs;

namespace Route.Algorithms.Default.EdgeBased
{
    /// <summary>
    /// An algorithm to calculate many-to-many directed weights.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DirectedManyToManyWeights<T> : AlgorithmBase
        where T : struct
    {
        private readonly Graph _graph;
        private readonly WeightHandler<T> _weightHandler;
        private readonly DirectedDykstraSource<T>[] _sources;
        private readonly DirectedDykstraSource<T>[] _targets;
        private readonly T _maxSearch;
        private readonly RestrictionCollection _restrictions;

        /// <summary>
        /// Creates a new many-to-many algorithm instance.
        /// </summary>
        public DirectedManyToManyWeights(Graph graph, WeightHandler<T> weightHandler, RestrictionCollection restrictions,
            DirectedDykstraSource<T>[] sources, DirectedDykstraSource<T>[] targets, T maxSearch)
        {
            _graph = graph;
            _sources = sources;
            _targets = targets;
            _weightHandler = weightHandler;
            _maxSearch = maxSearch;
            _restrictions = restrictions;
        }

        private T[][] _weights;

        /// <summary>
        /// Executes the actual algorithm.
        /// </summary>
        protected override void DoRun(CancellationToken cancellationToken)
        {
            _weights = new T[_sources.Length][];

            // do a one-to-many search for each source.
            // TODO: investigate doing this in a multithreaded way.
            for (var i = 0; i < _sources.Length; i++)
            {
                var oneToMany = new DirectedOneToManyWeights<T>(_graph, _weightHandler, _restrictions, _sources[i], _targets, _maxSearch);
                oneToMany.Run(cancellationToken);

                _weights[i] = oneToMany.Weights;
            }

            this.HasSucceeded = true;
        }

        /// <summary>
        /// Gets the weights.
        /// </summary>
        public T[][] Weights
        {
            get
            {
                return _weights;
            }
        }
    }
}