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

using System.Collections.Generic;
using System.Threading;
using Route.Algorithms.Search;
using Route.Algorithms.Weights;
using Route.LocalGeo;
using Route.Profiles;

namespace Route.Algorithms.Matrices
{
    /// <summary>
    /// An algorithm to calculate a weight-matrix for a set of locations.
    /// </summary>
    public class WeightMatrixAlgorithm<T> : AlgorithmBase, IWeightMatrixAlgorithm<T>
        where T : struct
    {
        protected readonly RouterBase _router;
        protected readonly IProfileInstance _profile;
        protected readonly WeightHandler<T> _weightHandler;
        protected readonly IMassResolvingAlgorithm _massResolver;
        protected readonly RoutingSettings<T> _settings;

        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, WeightHandler<T> weightHandler, Coordinate[] locations)
            : this(router, profile, weightHandler, new MassResolvingAlgorithm(
                router, new IProfileInstance[] { profile }, locations))
        {

        }

        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, WeightHandler<T> weightHandler,
            List<RouterPoint> resolvedLocations)
            : this(router, profile, weightHandler, new PresolvedMassResolvingAlgorithm(
                router, new IProfileInstance[] { profile }, resolvedLocations))
        {

        }

        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, WeightHandler<T> weightHandler, IMassResolvingAlgorithm massResolver)
        : this(router, profile, weightHandler, massResolver, null)
        {
            
        }

        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, WeightHandler<T> weightHandler, IMassResolvingAlgorithm massResolver,
            RoutingSettings<T> settings)
        {
            _router = router;
            _profile = profile;
            _weightHandler = weightHandler;
            _massResolver = massResolver;
            _settings = settings;
        }

        protected Dictionary<int, RouterPointError> _errors; // all errors per router point idx.
        protected List<int> _correctedIndices; // the original router point per resolved point index.
        protected List<RouterPoint> _correctedResolvedPoints; // only the valid resolved points.
        protected T[][] _weights; // the weights between all valid resolved points.        
        
        /// <summary>
        /// Executes the algorithm.
        /// </summary>
        protected override void DoRun(CancellationToken cancellationToken)
        {
            // run mass resolver if needed.
            if (!_massResolver.HasRun)
            {
                _massResolver.Run(cancellationToken);
            }

            // create error and resolved point management data structures.
            _correctedResolvedPoints = _massResolver.RouterPoints;
            _errors = new Dictionary<int, RouterPointError>(_correctedResolvedPoints.Count);
            _correctedIndices = new List<int>(_correctedResolvedPoints.Count);
            for (var i = 0; i < _correctedResolvedPoints.Count; i++)
            {
                _correctedIndices.Add(i);
            }
            
            // calculate matrix.
            var nonNullInvalids = new HashSet<int>();
            var locations = _correctedResolvedPoints.ToArray();
            var weightsResult = _router.TryCalculateWeight(_profile, _weightHandler, locations, locations, 
                nonNullInvalids, nonNullInvalids, _settings);
            _weights = weightsResult.Value;

            // take into account the non-null invalids now.
            if (nonNullInvalids.Count > 0)
            { // shrink lists and add errors.
                foreach (var invalid in nonNullInvalids)
                {
                    _errors[invalid] = new RouterPointError()
                    {
                        Code = RouterPointErrorCode.NotRoutable,
                        Message = "Location could not routed to or from."
                    };
                }

                _correctedResolvedPoints = _correctedResolvedPoints.ShrinkAndCopyList(nonNullInvalids);
                _correctedIndices = _correctedIndices.ShrinkAndCopyList(nonNullInvalids);
                _weights = _weights.SchrinkAndCopyMatrix(nonNullInvalids);
            }
            this.HasSucceeded = true;
        }
        
        /// <summary>
        /// Gets the router.
        /// </summary>
        public RouterBase Router
        {
            get
            {
                return _router;
            }
        }

        /// <summary>
        /// Gets the profile.
        /// </summary>
        public IProfileInstance Profile
        {
            get
            {
                return _profile;
            }
        }

        /// <summary>
        /// Gets the mass resolver.
        /// </summary>
        public IMassResolvingAlgorithm MassResolver
        {
            get
            {
                return _massResolver;
            }
        }

        /// <summary>
        /// Gets the weights between all valid router points.
        /// </summary>
        public T[][] Weights
        {
            get
            {
                this.CheckHasRunAndHasSucceeded();

                return _weights;
            }
        }

        /// <summary>
        /// Gets the valid router points.
        /// </summary>
        public List<RouterPoint> RouterPoints
        {
            get
            {
                this.CheckHasRunAndHasSucceeded();

                return _correctedResolvedPoints;
            }
        }
        
        /// <summary>
        /// Returns the corrected index, or the index in the weight matrix for the given routerpoint index.
        /// </summary>
        /// <param name="resolvedIdx">The index of the resolved point.</param>
        /// <returns>The index in the weight matrix, -1 if this point is in error.</returns>
        public int CorrectedIndexOf(int resolvedIdx)
        {
            this.CheckHasRunAndHasSucceeded();

            return _correctedIndices.IndexOf(resolvedIdx);
        }
        
        /// <summary>
        /// Returns the route rpoint index that represents the given weight in the weight matrix.
        /// </summary>
        /// <param name="weightIdx">The index in the weight matrix.</param>
        /// <returns>The router point index, always exists and always returns a proper value.</returns>
        public int OriginalIndexOf(int weightIdx)
        {
            this.CheckHasRunAndHasSucceeded();

            return _correctedIndices[weightIdx];
        }

        /// <summary>
        /// Returns the errors indexed per original router point index.
        /// </summary>
        public Dictionary<int, RouterPointError> Errors
        {
            get
            {
                this.CheckHasRunAndHasSucceeded();

                return _errors;
            }
        }
    }

    /// <summary>
    /// An algorithm to calculate a weight-matrix for a set of locations.
    /// </summary>
    public sealed class WeightMatrixAlgorithm : WeightMatrixAlgorithm<float>
    {
        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, IMassResolvingAlgorithm massResolver)
            : base(router, profile, profile.DefaultWeightHandler(router), massResolver)
        {

        }
        
        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, Coordinate[] locations)
            : base(router, profile, profile.DefaultWeightHandler(router), locations)
        {

        }
        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, List<RouterPoint> resolvedLocations)
            : base(router, profile, profile.DefaultWeightHandler(router), resolvedLocations)
        {

        }

        /// <summary>
        /// Creates a new weight-matrix algorithm.
        /// </summary>
        public WeightMatrixAlgorithm(RouterBase router, IProfileInstance profile, IMassResolvingAlgorithm massResolver,
            RoutingSettings<float> settings)
            : base(router, profile, profile.DefaultWeightHandler(router), massResolver, settings)
        {
            
        }
        
        /// <summary>
        /// Executes the algorithm.
        /// </summary>
        protected override void DoRun(CancellationToken cancellationToken)
        {
            // run mass resolver if needed.
            if (!_massResolver.HasRun)
            {
                _massResolver.Run(cancellationToken);
            }

            // create error and resolved point management data structures.
            _correctedResolvedPoints = _massResolver.RouterPoints;
            _errors = new Dictionary<int, RouterPointError>(_correctedResolvedPoints.Count);
            _correctedIndices = new List<int>(_correctedResolvedPoints.Count);
            for (var i = 0; i < _correctedResolvedPoints.Count; i++)
            {
                _correctedIndices.Add(i);
            }
            
            // calculate matrix.
            var nonNullInvalids = new HashSet<int>();
            var locations = _correctedResolvedPoints.ToArray();
            var weightsResult = _router.TryCalculateWeight(_profile, _weightHandler, locations, locations, 
                nonNullInvalids, nonNullInvalids, _settings);
            _weights = weightsResult.Value;

            // take into account the non-null invalids now.
            if (nonNullInvalids.Count > 0)
            { // shrink lists and add errors.
                foreach (var invalid in nonNullInvalids)
                {
                    _errors[invalid] = new RouterPointError()
                    {
                        Code = RouterPointErrorCode.NotRoutable,
                        Message = "Location could not routed to or from."
                    };
                }

                _correctedResolvedPoints = _correctedResolvedPoints.ShrinkAndCopyList(nonNullInvalids);
                _correctedIndices = _correctedIndices.ShrinkAndCopyList(nonNullInvalids);
                _weights = _weights.SchrinkAndCopyMatrix(nonNullInvalids);
            }
            this.HasSucceeded = true;
        }
    }
}