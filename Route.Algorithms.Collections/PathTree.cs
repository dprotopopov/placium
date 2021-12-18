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

namespace Route.Algorithms.Collections
{
    /// <summary>
    /// Represents a tree of paths by linking their segments together.
    /// </summary>
    public sealed class PathTree
    {
        /// <summary>
        /// Creates a new path tree.
        /// </summary>
        public PathTree()
        {
            _data = new long[1024];
        }

        private long[] _data;
        private long _pointer;

        /// <summary>
        /// Adds a new segment.
        /// </summary>
        /// <returns></returns>
        public long Add(long data0, long data1)
        {
            var id = _pointer;
            if (_data.Length <= _pointer + 2)
            {
                System.Array.Resize(ref _data, _data.Length * 2);
            }
            _data[_pointer + 0] = data0;
            _data[_pointer + 1] = data1;
            _pointer += 2;
            return id;
        }

        /// <summary>
        /// Adds a new segment.
        /// </summary>
        /// <returns></returns>
        public long Add(long data0, long data1, long data2)
        {
            var id = _pointer;
            if (_data.Length <= _pointer + 3)
            {
                System.Array.Resize(ref _data, _data.Length * 2);
            }
            _data[_pointer + 0] = data0;
            _data[_pointer + 1] = data1;
            _data[_pointer + 2] = data2;
            _pointer += 3;
            return id;
        }

        /// <summary>
        /// Adds a new segment.
        /// </summary>
        /// <returns></returns>
        public long Add(long data0, long data1, long data2, long data3)
        {
            var id = _pointer;
            if (_data.Length <= _pointer + 4)
            {
                System.Array.Resize(ref _data, _data.Length * 2);
            }
            _data[_pointer + 0] = data0;
            _data[_pointer + 1] = data1;
            _data[_pointer + 2] = data2;
            _data[_pointer + 3] = data3;
            _pointer += 4;
            return id;
        }

        /// <summary>
        /// Adds a new segment.
        /// </summary>
        /// <returns></returns>
        public long Add(long data0, long data1, long data2, long data3, long data4)
        {
            var id = _pointer;
            if (_data.Length <= _pointer + 5)
            {
                System.Array.Resize(ref _data, _data.Length * 2);
            }
            _data[_pointer + 0] = data0;
            _data[_pointer + 1] = data1;
            _data[_pointer + 2] = data2;
            _data[_pointer + 3] = data3;
            _data[_pointer + 4] = data4;
            _pointer += 5;
            return id;
        }

        /// <summary>
        /// Adds a new segment.
        /// </summary>
        /// <returns></returns>
        public long Add(long data0, long data1, long data2, long data3, long data4, long data5)
        {
            var id = _pointer;
            if (_data.Length <= _pointer + 6)
            {
                System.Array.Resize(ref _data, _data.Length * 2);
            }
            _data[_pointer + 0] = data0;
            _data[_pointer + 1] = data1;
            _data[_pointer + 2] = data2;
            _data[_pointer + 3] = data3;
            _data[_pointer + 4] = data4;
            _data[_pointer + 5] = data5;
            _pointer += 6;
            return id;
        }

        /// <summary>
        /// Adds a new segment.
        /// </summary>
        /// <returns></returns>
        public long Add(long data0, long data1, long data2, long data3, long data4, long data5, long data6)
        {
            var id = _pointer;
            if (_data.Length <= _pointer + 7)
            {
                System.Array.Resize(ref _data, _data.Length * 2);
            }
            _data[_pointer + 0] = data0;
            _data[_pointer + 1] = data1;
            _data[_pointer + 2] = data2;
            _data[_pointer + 3] = data3;
            _data[_pointer + 4] = data4;
            _data[_pointer + 5] = data5;
            _data[_pointer + 6] = data6;
            _pointer += 7;
            return id;
        }

        /// <summary>
        /// Gets the data at the given pointer.
        /// </summary>
        public void Get(long pointer, out long data0, out long data1)
        {
            data0 = _data[pointer + 0];
            data1 = _data[pointer + 1];
        }

        /// <summary>
        /// Gets the data at the given pointer.
        /// </summary>
        public void Get(long pointer, out long data0, out long data1, out long data2)
        {
            data0 = _data[pointer + 0];
            data1 = _data[pointer + 1];
            data2 = _data[pointer + 2];
        }

        /// <summary>
        /// Gets the data at the given pointer.
        /// </summary>
        public void Get(long pointer, out long data0, out long data1, out long data2,
            out long data3)
        {
            data0 = _data[pointer + 0];
            data1 = _data[pointer + 1];
            data2 = _data[pointer + 2];
            data3 = _data[pointer + 3];
        }

        /// <summary>
        /// Gets the data at the given pointer.
        /// </summary>
        public void Get(long pointer, out long data0, out long data1, out long data2,
            out long data3, out long data4)
        {
            data0 = _data[pointer + 0];
            data1 = _data[pointer + 1];
            data2 = _data[pointer + 2];
            data3 = _data[pointer + 3];
            data4 = _data[pointer + 4];
        }

        /// <summary>
        /// Gets the data at the given pointer.
        /// </summary>
        public void Get(long pointer, out long data0, out long data1, out long data2,
            out long data3, out long data4, out long data5)
        {
            data0 = _data[pointer + 0];
            data1 = _data[pointer + 1];
            data2 = _data[pointer + 2];
            data3 = _data[pointer + 3];
            data4 = _data[pointer + 4];
            data5 = _data[pointer + 5];
        }

        /// <summary>
        /// Gets the data at the given pointer.
        /// </summary>
        public void Get(long pointer, out long data0, out long data1, out long data2,
            out long data3, out long data4, out long data5, out long data6)
        {
            data0 = _data[pointer + 0];
            data1 = _data[pointer + 1];
            data2 = _data[pointer + 2];
            data3 = _data[pointer + 3];
            data4 = _data[pointer + 4];
            data5 = _data[pointer + 5];
            data6 = _data[pointer + 6];
        }

        /// <summary>
        /// Clears all data.
        /// </summary>
        public void Clear()
        {
            _pointer = 0;
        }
    }
}