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
using Route.Algorithms.Weights;
using Route.Graphs.Directed;

namespace Route.Data.Contracted.Edges
{
    /// <summary>
    /// Parses edge data.
    /// </summary>
    public static class ContractedEdgeDataSerializer
    {
        /// <summary>
        /// Holds the precision-factor.
        /// </summary>
        public const int PRECISION_FACTOR = 10;

        /// <summary>
        /// Holds the maxium distance that can be stored on an edge.
        /// </summary>
        public static float MAX_DISTANCE = 4294967000 / 4 / PRECISION_FACTOR;

        /// <summary>
        /// Deserializes edges data.
        /// </summary>
        /// <returns></returns>
        public static ContractedEdgeData Deserialize(long data, long metaData)
        {
            float weight;
            bool? direction;
            long contractedId;
            ContractedEdgeDataSerializer.Deserialize(data, metaData,
                out weight, out direction, out contractedId);

            return new ContractedEdgeData()
            {
                ContractedId = contractedId,
                Weight = weight,
                Direction = direction
            };
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static void Deserialize(long data0, long data1, out float weight, out bool? direction,
            out long contractedId)
        {
            var dirFlags = (data0 & 3);
            direction = null;
            if(dirFlags == 1)
            {
                direction = true;
            }
            else if(dirFlags == 2)
            {
                direction = false;
            }
            weight = ((data0 - dirFlags) / 4.0f) / (float)PRECISION_FACTOR;
            contractedId = data1;
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        public static void DeserializeDynamic(long[] data, out float weight, out bool? direction, out float distance, out float time)
        {
            ContractedEdgeDataSerializer.Deserialize(data[0], out weight, out direction);
            distance = data[1] / 10.0f;
            time = data[2];
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static void Deserialize(long data0, out float weight, out bool? direction)
        {
            var dirFlags = (data0 & 3);
            direction = null;
            if (dirFlags == 1)
            {
                direction = true;
            }
            else if (dirFlags == 2)
            {
                direction = false;
            }
            weight = ((data0 - dirFlags) / 4.0f) / (float)PRECISION_FACTOR;
        }

        /// <summary>
        /// Deserializes the agugmented data.
        /// </summary>
        public static void DeserializeMetaAgumented(long[] data, out long contractedId, out float distance, out float time)
        {
            contractedId = data[0];
            distance = data[1] / 10.0f;
            time = data[2];
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static Func<long[], float> DeserializeWeightFunc = (data) =>
            {
                return ContractedEdgeDataSerializer.DeserializeWeight(data[0]);
            };

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static float DeserializeWeight(long data)
        {
            float weight;
            bool? direction;
            ContractedEdgeDataSerializer.Deserialize(data, out weight, out direction);
            return weight;
        }

        /// <summary>
        /// Returns true if the data represents the same direction.
        /// </summary>
        /// <returns></returns>
        public static bool HasDirection(long data, bool? direction)
        {
            float weight;
            bool? currentDirection;
            ContractedEdgeDataSerializer.Deserialize(data, out weight, out currentDirection);
            return currentDirection == direction;
        }

        /// <summary>
        /// Returns the size of a the meta data in long's when serialized.
        /// </summary>
        public static int MetaSize
        {
            get { return 1; }
        }

        /// <summary>
        /// Returns the size of a the meta data in long's when serialized and including augmented weights.
        /// </summary>
        public static int MetaAugmentedSize
        {
            get { return 3; }
        }

        /// <summary>
        /// Returns the size of the fixed component of the data in the dynamic graph.
        /// </summary>
        public static int DynamicFixedSize
        {
            get { return 1; }
        }

        /// <summary>
        /// Returns the size of the fixed component of the data in the dynamic graph including augmented weights.
        /// </summary>
        public static int DynamicAugmentedFixedSize
        {
            get { return 3; }
        }

        /// <summary>
        /// Returns the size of a the data in long's when serialized.
        /// </summary>
        public static int Size
        {
            get { return 1; }
        }

        /// <summary>
        /// Serializes edge data.
        /// </summary>
        /// <returns></returns>
        public static long Serialize(float weight, bool? direction)
        {
            if (weight > MAX_DISTANCE)
            {
                throw new ArgumentOutOfRangeException(nameof(weight), "Cannot store distance on edge, too big.");
            }
            if (weight < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(weight),"Cannot store distance on edge, too small.");
            }

            var dirFlags = 0;
            if(direction.HasValue && direction.Value)
            {
                dirFlags = 1;
            }
            else if(direction.HasValue && !direction.Value)
            {
                dirFlags = 2;
            }

            var data0 = (long)dirFlags;
            data0 = data0 + ((long)(weight * PRECISION_FACTOR) * 4);
            return data0;
        }

        /// <summary>
        /// Serializes time.
        /// </summary>
        public static long SerializeTime(float time)
        {
            return (long)time;
        }

        /// <summary>
        /// Serializes distance.
        /// </summary>
        public static long SerializeDistance(float distance)
        {
            return (long)(distance * 10);
        }

        /// <summary>
        /// Serializes augmented edge data.
        /// </summary>
        /// <returns></returns>
        public static long[] SerializeDynamicAugmented(float weight, bool? direction, float distance, float time)
        { // precision of 0.1m and 1 second.
            return new long[] {
                Serialize(weight, direction),
                SerializeDistance(distance),
                SerializeTime(time)
            };
        }

        /// <summary>
        /// Serializes augmented edge data.
        /// </summary>
        /// <returns></returns>
        public static long[] SerializeMetaAugmented(long contractedId, float distance, float time)
        { // precision of 0.1m and 1 second.
            return new long[] {
                contractedId,
                SerializeDistance(distance),
                (long)time
            };
        }

        /// <summary>
        /// Serializes edge data.
        /// </summary>
        /// <returns></returns>
        public static long[] SerializeMeta(ContractedEdgeData data)
        {
            return new long[]
            {
                ContractedEdgeDataSerializer.Serialize(data.Weight, data.Direction),
                data.ContractedId
            };
        }

        /// <summary>
        /// Gets contracted edge data.
        /// </summary>
        /// <returns></returns>
        public static ContractedEdgeData GetContractedEdgeData(this MetaEdge edge)
        {
            float weight;
            bool? direction;
            long contractedId;
            ContractedEdgeDataSerializer.Deserialize(edge.Data[0], edge.MetaData[0],
                out weight, out direction, out contractedId);
            return new ContractedEdgeData()
            {
                ContractedId = contractedId,
                Direction = direction,
                Weight = weight
            };
        }

        /// <summary>
        /// Gets contracted id.
        /// </summary>
        /// <returns></returns>
        public static long GetContractedId(this MetaEdge edge)
        {
            return edge.MetaData[0];
        }

        /// <summary>
        /// Gets contracted id.
        /// </summary>
        /// <returns></returns>
        public static long GetContractedId(this DirectedMetaGraph.EdgeEnumerator edge)
        {
            return edge.MetaData0;
        }

        /// <summary>
        /// Gets contracted id.
        /// </summary>
        /// <returns></returns>
        public static long GetContractedId(this long[] metaData)
        {
            return metaData[0];
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static void Deserialize(long data0, out Dir direction, out float weight)
        {
            // TODO: change layout in future version to simplify this.
            var dirFlags = (data0 & 3);
            if (dirFlags == 1)
            {
                direction = new Dir(1);
            }
            else if (dirFlags == 2)
            {
                direction = new Dir(2);
            }
            else
            {
                direction = new Dir(3);
            }
            weight = ((data0 - dirFlags) >> 2) / (float)PRECISION_FACTOR;
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static void Deserialize(long data0, out Dir direction, out long weight)
        {
            // TODO: change layout in future version to simplify this.
            var dirFlags = (data0 & 3);
            if (dirFlags == 1)
            {
                direction = new Dir(1);
            }
            else if (dirFlags == 2)
            {
                direction = new Dir(2);
            }
            else
            {
                direction = new Dir(3);
            }
            weight = data0 >> 2;
        }

        /// <summary>
        /// Parses the edge data.
        /// </summary>
        /// <returns></returns>
        public static WeightAndDir<float> Deserialize(long data0)
        {
            // TODO: change layout in future version to simplify this.
            Dir direction;

            var dirFlags = (data0 & 3);
            if (dirFlags == 1)
            {
                direction = new Dir(1);
            }
            else if (dirFlags == 2)
            {
                direction = new Dir(2);
            }
            else
            {
                direction = new Dir(3);
            }

            return new WeightAndDir<float>()
            {
                Direction = direction,
                Weight = ((data0 - dirFlags) >> 2) / (float)PRECISION_FACTOR
            };
        }
    }
}