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

using Route.Algorithms.Collections;
using Route.Algorithms.Weights;

namespace Route.Algorithms.Contracted
{
    /// <summary>
    /// Contains extension methods related to path tree.
    /// </summary>
    public static class PathTreeExtensions
    {
        private const float WeightFactor = 1000;
        
        /// <summary>
        /// Adds a new settled vertex.
        /// </summary>
        public static long AddSettledVertex(this PathTree tree, long vertex, WeightAndDir<float> weightAndDir, long hops)
        {
            var hopsAndDirection = hops * 4 + weightAndDir.Direction._val;
            return tree.Add(vertex, (long)(weightAndDir.Weight * WeightFactor),
                hopsAndDirection);
        }

        /// <summary>
        /// Adds a new settled vertex.
        /// </summary>
        public static long AddSettledVertex(this PathTree tree, long vertex, WeightAndDir<float> weightAndDir, long hops, long pPointer)
        {
            var hopsAndDirection = hops * 4 + weightAndDir.Direction._val;
            return tree.Add(vertex, (long)(weightAndDir.Weight * WeightFactor),
                hopsAndDirection, pPointer);
        }

        /// <summary>
        /// Adds a new settled vertex.
        /// </summary>
        public static long AddSettledVertex(this PathTree tree, long vertex, float weight, Dir dir, long hops)
        {
            var hopsAndDirection = hops * 4 + dir._val;
            return tree.Add(vertex, (long)(weight * WeightFactor),
                hopsAndDirection);
        }

        /// <summary>
        /// Adds a new settled vertex.
        /// </summary>
        public static long AddSettledVertex(this PathTree tree, long vertex, float weight, Dir dir, long hops, long pPointer)
        {
            var hopsAndDirection = hops * 4 + dir._val;
            return tree.Add(vertex, (long)(weight * WeightFactor),
                hopsAndDirection, pPointer);
        }

        /// <summary>
        /// Gets a settled vertex.
        /// </summary>
        public static void GetSettledVertex(this PathTree tree, long pointer, out long vertex,
            out WeightAndDir<float> weightAndDir, out long hops)
        {
            long data0, data1, data2;
            tree.Get(pointer, out data0, out data1, out data2);
            vertex = data0;
            weightAndDir = new WeightAndDir<float>()
            {
                Weight = data1 / WeightFactor,
                Direction = new Dir()
                {
                    _val = (byte)(data2 & 3)
                }
            };
            hops = data2 / 4;
        }

        /// <summary>
        /// Gets a settled vertex.
        /// </summary>
        public static void GetSettledVertex(this PathTree tree, long pointer, out long vertex,
            out WeightAndDir<float> weightAndDir, out long hops, out long previous)
        {
            long data0, data1, data2, data3;
            tree.Get(pointer, out data0, out data1, out data2, out data3);
            vertex = data0;
            previous = data3;
            weightAndDir = new WeightAndDir<float>()
            {
                Weight = data1 / WeightFactor,
                Direction = new Dir()
                {
                    _val = (byte)(data2 & 3)
                }
            };
            hops = data2 / 4;
        }

        /// <summary>
        /// Gets a settled vertex weight.
        /// </summary>
        public static WeightAndDir<float> GetSettledVertexWeight(this PathTree tree, long pointer)
        {
            long data0, data1, data2;
            tree.Get(pointer, out data0, out data1, out data2);
            return new WeightAndDir<float>()
            {
                Weight = data1 / WeightFactor,
                Direction = new Dir()
                {
                    _val = (byte)(data2 & 3)
                }
            };
        }
    }
}