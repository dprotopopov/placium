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

namespace Route.Algorithms.Weights
{
    /// <summary>
    /// Represents a vertex and associated weight.
    /// </summary>
    public struct VertexAndWeight<T>
        where T : struct
    {
        /// <summary>
        /// Creates a new vertex and weight with weight zero.
        /// </summary>
        public VertexAndWeight(long vertex)
        {
            this.Vertex = vertex;
            this.Weight = default(T);
        }           

        /// <summary>
        /// Gets or sets the vertex.
        /// </summary>
        public long Vertex { get; set; }

        /// <summary>
        /// Gets or sets the weight.
        /// </summary>
        public T Weight { get; set; }
    }
}