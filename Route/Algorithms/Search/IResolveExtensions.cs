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
using Route.Data.Network;
using Route.Graphs.Geometric;

namespace Route.Algorithms.Search
{
    /// <summary>
    /// Contains extensions and helper functions related to resolvers.
    /// </summary>
    public static class IResolveExtensions
    {
        /// <summary>
        /// Delegate to create a resolver.
        /// </summary>
        public delegate IResolver CreateResolver(float latitude, float longitude, Func<GeometricEdge, bool> isAcceptable, Func<RoutingEdge, bool> isBetter);
    }
}