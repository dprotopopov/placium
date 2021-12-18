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
    /// Represents a large bit array.
    /// </summary>
    public class BitArray64
    {
        private readonly long[] _array; // Holds the bit vector array.
        private long _length; // Holds the length of this array.

        /// <summary>
        /// Creates a new bitvector array.
        /// </summary>
        public BitArray64(long size)
        {
            _length = size;
            _array = new long[(int)System.Math.Ceiling((double)size / 64)];
        }

        /// <summary>
        /// Returns the element at the given index.
        /// </summary>
        public bool this[long idx]
        {
            get
            {
                var arrayIdx = (int)(idx >> 6);
                var bitIdx = (int)(idx % 64);
                var mask = (long)1 << bitIdx;
                return (_array[arrayIdx] & mask) != 0;
            }
            set
            {
                var arrayIdx = (int)(idx >> 6);
                var bitIdx = (int)(idx % 64);
                var mask = (long)1 << bitIdx;
                if (value)
                { // set value.
                    _array[arrayIdx] = (long)(mask | _array[arrayIdx]);
                }
                else
                { // unset value.
                    _array[arrayIdx] = (long)((~mask) & _array[arrayIdx]);
                }
            }
        }

        /// <summary>
        /// Returns the length of this array.
        /// </summary>
        public long Length
        {
            get
            {
                return _length;
            }
        }
    }
}