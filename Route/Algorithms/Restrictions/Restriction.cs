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

namespace Route.Algorithms.Restrictions
{
    /// <summary>
    /// Represents a restriction.
    /// </summary>
    public struct Restriction
    {
        /// <summary>
        /// Creates a restriction of size 1.
        /// </summary>
        public Restriction(long vertex)
        {
            this.Vertex1 = vertex;
            this.Vertex2 = Constants.NO_VERTEX;
            this.Vertex3 = Constants.NO_VERTEX;
        }

        /// <summary>
        /// Creates a restriction of size 2.
        /// </summary>
        public Restriction(long vertex1, long vertex2)
        {
            this.Vertex1 = vertex1;
            this.Vertex2 = vertex2;
            this.Vertex3 = Constants.NO_VERTEX;
        }

        /// <summary>
        /// Creates a restriction of size 3.
        /// </summary>
        public Restriction(long vertex1, long vertex2, long vertex3)
        {
            this.Vertex1 = vertex1;
            this.Vertex2 = vertex2;
            this.Vertex3 = vertex3;
        }

        /// <summary>
        /// First vertex.
        /// </summary>
        public long Vertex1 { get; set; }

        /// <summary>
        /// Second vertex.
        /// </summary>
        public long Vertex2 { get; set; }

        /// <summary>
        /// Third vertex.
        /// </summary>
        public long Vertex3 { get; set; }
    }
}