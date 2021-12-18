/*
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
using Route.Graphs.Directed;

namespace Route.Algorithms.Contracted.EdgeBased.Witness
{
    /// <summary>
    /// Calculator to calculate witness paths.
    /// </summary>
    public interface IWitnessCalculator<T>
        where T : struct
    {
        /// <summary>
        /// Calculates witnesses.
        /// </summary>
        void Calculate(DirectedDynamicGraph graph, Func<long, IEnumerable<long[]>> getRestrictions, long source, List<long> targets, List<T> weights,
            ref EdgePath<T>[] forwardWitness, ref EdgePath<T>[] backwardWitness, long vertexToSkip);
    }
}