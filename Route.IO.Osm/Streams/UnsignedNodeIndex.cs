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

using Reminiscence.Arrays;
using Route.Algorithms.Sorting;
using Route.LocalGeo;
using Route.Logging;

namespace Route.IO.Osm.Streams
{
    /// <summary>
    ///     A cache for node coordinates.
    /// </summary>
    internal sealed class UnsignedNodeIndex
    {
        private readonly ArrayBase<long> _vertex;
        private readonly ArrayBase<long> _index;
        private readonly ArrayBase<bool> _isCoordsExist;
        private readonly ArrayBase<bool> _isCoreVertex;

        private readonly ArrayBase<float> _lat;
        private readonly ArrayBase<float> _lon;
        private long _idx;


        /// <summary>
        ///     Creates a new node coordinates cache.
        /// </summary>
        public UnsignedNodeIndex()
        {
            _index = Context.ArrayFactory.CreateMemoryBackedArray<long>(1024 * 1024);
            _vertex = Context.ArrayFactory.CreateMemoryBackedArray<long>(0);
            _isCoreVertex = Context.ArrayFactory.CreateMemoryBackedArray<bool>(0);
            _isCoordsExist = Context.ArrayFactory.CreateMemoryBackedArray<bool>(0);
            _lat = Context.ArrayFactory.CreateMemoryBackedArray<float>(0);
            _lon = Context.ArrayFactory.CreateMemoryBackedArray<float>(0);
        }

        /// <summary>
        ///     Gets the node id at the given index.
        /// </summary>
        public long this[long idx] => _index[idx];

        /// <summary>
        ///     Returns the number of elements.
        /// </summary>
        public long Count => _index.Length;

        /// <summary>
        ///     Adds a node id to the index.
        /// </summary>
        public void AddId(long id)
        {
            _index.EnsureMinimumSize(_idx + 1);
            _index[_idx++] = id;
        }


        /// <summary>
        ///     Sorts and converts the index.
        /// </summary>
        public void SortAndConvertIndex()
        {
            _index.Resize(_idx);

            Logger.Log("NodeIndex", TraceEventType.Information, "Sorting node id's...");
            QuickSort.Sort(i => { return _index[i]; },
                (i, j) =>
                {
                    var int1 = _index[i];
                    _index[i] = _index[j];
                    _index[j] = int1;
                }, 0, _index.Length - 1);

            _idx = _index.Length;
        }

        /// <summary>
        ///     Sets a vertex id for the given vertex.
        /// </summary>
        public void Set(long id, long vertex)
        {
            var idx = TryGetIndex(id);

            _vertex.EnsureMinimumSize(idx + 1, int.MaxValue);
            _isCoreVertex.EnsureMinimumSize(idx + 1, false);
            _vertex[idx] = vertex;
            _isCoreVertex[idx] = true;
        }

        /// <summary>
        ///     Sets the coordinate for the given index.
        /// </summary>
        public void SetIndex(long idx, float latitude, float longitude)
        {
            _lat.EnsureMinimumSize(idx + 1, float.MaxValue);
            _lon.EnsureMinimumSize(idx + 1, float.MaxValue);
            _isCoordsExist.EnsureMinimumSize(idx + 1, false);
            _isCoreVertex.EnsureMinimumSize(idx + 1, false);

            if (_isCoreVertex.Length >= idx && _isCoreVertex[idx])
                // this is already a core vertex, no need to overwrite this more valuable data.
                return;

            _isCoordsExist[idx] = true;
            _lat[idx] = latitude;
            _lon[idx] = longitude;
        }

        /// <summary>
        ///     Tries to get a core node and it's matching vertex.
        /// </summary>
        public bool TryGetCoreNode(long id, out long vertex)
        {
            var idx = TryGetIndex(id);
            if (idx == long.MaxValue)
            {
                vertex = long.MaxValue;
                return false;
            }

            if (_vertex.Length <= idx)
            {
                vertex = long.MaxValue;
                return false;
            }

            vertex = _vertex[idx];
            return _isCoreVertex.Length > idx && _isCoreVertex[idx];
        }

        /// <summary>
        ///     Returns true if the given id is a core node.
        /// </summary>
        public bool IsCoreNode(long id)
        {
            var idx = TryGetIndex(id);
            if (idx != long.MaxValue)
            {
                if (IsCoreNodeAtIndex(idx, id)) return true;
                return false;
            }

            return false;
        }


        /// <summary>
        ///     Returns true if the given id is in this index.
        /// </summary>
        public bool HasId(long id)
        {
            var idx = TryGetIndex(id);
            return idx != long.MaxValue;
        }

        /// <summary>
        ///     Gets the coordinate for the given node.
        /// </summary>
        public bool TryGetValue(long id, out float latitude, out float longitude, out bool isCore)
        {
            var idx = TryGetIndex(id);
            if (idx == long.MaxValue)
            {
                latitude = float.MaxValue;
                longitude = float.MaxValue;
                isCore = false;
                return false;
            }

            if (!GetLatLon(idx, out latitude, out longitude))
            {
                latitude = float.MaxValue;
                longitude = float.MaxValue;
                isCore = false;
                return false;
            }

            isCore = IsCoreNodeAtIndex(idx, id);
            return true;
        }

        /// <summary>
        ///     Gets all relevant info on the given node.
        /// </summary>
        public bool TryGetValue(long id, out float latitude, out float longitude, out bool isCore, out long vertex)
        {
            var idx = TryGetIndex(id);
            if (idx == long.MaxValue)
            {
                // no relevant data here.
                latitude = float.MaxValue;
                longitude = float.MaxValue;
                isCore = false;
                vertex = long.MaxValue;
                return false;
            }

            if (_isCoreVertex.Length >= idx && _isCoreVertex[idx])
            {
                // this is a core-vertex, no coordinates here anymore.
                latitude = float.MaxValue;
                longitude = float.MaxValue;
                isCore = IsCoreNodeAtIndex(idx, id);
                vertex = _vertex[idx];
                return true;
            }

            if (GetLatLon(idx, out latitude, out longitude))
            {
                // no relevant data.
                isCore = IsCoreNodeAtIndex(idx, id);
                vertex = long.MaxValue;
                return true;
            }

            latitude = float.MaxValue;
            longitude = float.MaxValue;
            isCore = false;
            vertex = long.MaxValue;
            return false;
        }

        /// <summary>
        ///     Gets the coordinate for the given node.
        /// </summary>
        public bool TryGetValue(long id, out Coordinate coordinate, out bool isCore)
        {
            float latitude, longitude;
            if (TryGetValue(id, out latitude, out longitude, out isCore))
            {
                coordinate = new Coordinate
                {
                    Latitude = latitude,
                    Longitude = longitude
                };
                return true;
            }

            coordinate = new Coordinate();
            return false;
        }

        /// <summary>
        ///     Returns true if a the given index there is an id that represents a core node.
        /// </summary>
        private bool IsCoreNodeAtIndex(long idx, long id)
        {
            if (idx > 0 &&
                GetId(idx - 1) == id)
                return true;
            if (idx < _index.Length - 1 &&
                GetId(idx + 1) == id)
                return true;
            return false;
        }

        /// <summary>
        ///     Gets the coordinate for the given node.
        /// </summary>
        public long TryGetIndex(long id)
        {

            // do a binary search.
            long bottom = 0;
            var top = _idx - 1;
            var bottomId = GetId(bottom);
            if (id == bottomId)
            {
                return bottom;
            }

            var topId = GetId(top);
            if (id == topId)
            {
                while (top - 1 > 0 &&
                       GetId(top - 1) == id)
                    top--;
                return top;
            }

            while (top - bottom > 1)
            {
                var middle = (top - bottom) / 2 + bottom;
                var middleId = GetId(middle);
                if (middleId == id)
                {
                    while (middle - 1 > 0 &&
                           GetId(middle - 1) == id)
                        middle--;
                    return middle;
                }

                if (middleId > id)
                {
                    topId = middleId;
                    top = middle;
                }
                else
                {
                    bottomId = middleId;
                    bottom = middle;
                }
            }

            return long.MaxValue;
        }

        private long GetId(long index)
        {
            if (_index.Length <= index) return long.MaxValue;

            return _index[index];
        }

        private bool GetLatLon(long idx, out float latitude, out float longitude)
        {
            if (_isCoordsExist.Length > idx && _isCoordsExist[idx])
            {
                latitude = _lat[idx];
                longitude = _lon[idx];
                return true;
            }

            latitude = float.MaxValue;
            longitude = float.MaxValue;
            return false;
        }
    }
}