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
using System.IO;
using Reminiscence.Arrays;
using Reminiscence.IO;
using Reminiscence.IO.Streams;

namespace Route.Data.Network.Restrictions
{
    /// <summary>
    /// Represents a restriction database.
    /// </summary>
    /// <remarks>A restriction is a sequence of vertices that is forbidden. A complex restriction is a restriction with > 1 vertex.</remarks>
    public class RestrictionsDb
    {
        private readonly ArrayBase<long> _hashes; // holds pointers to a set of pointers in the index.
        private readonly ArrayBase<long> _index; // holds pointers per hash to the actual restrictions.
        private readonly ArrayBase<long> _restrictions; // holds the actual restrictions with a prefixed count.
        private const int DEFAULT_ARRAY_SIZE = 1024;
        private const int DEFAULT_HASHCOUNT = 1024 * 1024;
        private const long NO_DATA = long.MaxValue;

        private bool _hasComplexRestrictions; // flag to indicate presence of complex restrictions.

        /// <summary>
        /// Creates a new restrictions db.
        /// </summary>
        public RestrictionsDb(int hashes = DEFAULT_HASHCOUNT)
        {
            _hasComplexRestrictions = false;
            _hashes = Context.ArrayFactory.CreateMemoryBackedArray<long>(hashes * 2);
            for (var i = 0; i < _hashes.Length; i++)
            {
                _hashes[i] = NO_DATA;
            }
            _restrictions = Context.ArrayFactory.CreateMemoryBackedArray<long>(DEFAULT_ARRAY_SIZE);
            _index = Context.ArrayFactory.CreateMemoryBackedArray<long>(DEFAULT_ARRAY_SIZE);
        }
        
        /// <summary>
        /// Creates a new restrictions db.
        /// </summary>
        private RestrictionsDb(long count, bool hasComplexRestrictions, ArrayBase<long> hashes, ArrayBase<long> index, ArrayBase<long> restrictions)
        {
            _count = (int)count;
            _hasComplexRestrictions = hasComplexRestrictions;
            _hashes = hashes;
            _index = index;
            _restrictions = restrictions;

            _nextIndexPointer = (long)index.Length;
            _nextRestrictionPointer = (long)_restrictions.Length;
        }
        
        private long _nextIndexPointer = 0;
        private long _nextRestrictionPointer = 0;
        private int _count = 0;

        /// <summary>
        /// Adds a new restriction.
        /// </summary>
        public void Add(params long[] restriction)
        {
            if (restriction == null) { throw new ArgumentNullException("restriction"); }
            if (restriction.Length == 0) { throw new ArgumentException("Restriction should contain one or more vertices.", "restriction"); }

            if (restriction.Length > 1)
            {
                _hasComplexRestrictions = true;
            }

            _restrictions.EnsureMinimumSize(_nextRestrictionPointer + (long)restriction.Length + 1);

            // add the data.
            _restrictions[_nextRestrictionPointer] = (long)restriction.Length;
            for (var i = 0; i < restriction.Length; i++)
            {
                _restrictions[_nextRestrictionPointer + i + 1] = restriction[i];
            }

            // add by pointer.
            this.AddByPointer(_nextRestrictionPointer);

            _nextRestrictionPointer = _nextRestrictionPointer + 1 + (long)restriction.Length;
            _count++;
        }

        /// <summary>
        /// Gets the number of restrictions in this db.
        /// </summary>
        public int Count
        {
            get
            {
                return _count;
            }
        }

        /// <summary>
        /// Switches the given two vertices.
        /// </summary>
        public void Switch(long vertex1, long vertex2)
        {
            // collect relevant restrictions.
            var restrictions = new HashSet<long>();
            var hash = CalculateHash(vertex1);
            var indexPointer = _hashes[hash];
            if (indexPointer != NO_DATA)
            {
                var pSize = _index[indexPointer];
                for (var i = 0; i < pSize; i++)
                {
                    var restrictionPointer = _index[indexPointer + i + 1];
                    var restrictionSize = _restrictions[restrictionPointer];
                    for (var j = 0; j < restrictionSize; j++)
                    {
                        if (_restrictions[restrictionPointer + 1 + j] == vertex1)
                        {
                            restrictions.Add(restrictionPointer);
                            break;
                        }
                    }
                }
            }
            hash = CalculateHash(vertex2);
            indexPointer = _hashes[hash];
            if (indexPointer != NO_DATA)
            {
                var pSize = _index[indexPointer];
                for (var i = 0; i < pSize; i++)
                {
                    var restrictionPointer = _index[indexPointer + i + 1];
                    var restrictionSize = _restrictions[restrictionPointer];
                    for (var j = 0; j < restrictionSize; j++)
                    {
                        if (_restrictions[restrictionPointer + 1 + j] == vertex2)
                        {
                            restrictions.Add(restrictionPointer);
                            break;
                        }
                    }
                }
            }

            // switch vertices and add/remove restrictions to update index/hashes.
            foreach(var restrictionPointer in restrictions)
            {
                this.RemoveByPointer(restrictionPointer);

                var restrictionSize = _restrictions[restrictionPointer];
                for (var i = 0; i < restrictionSize; i++)
                {
                    if (_restrictions[restrictionPointer + 1 + i] == vertex2)
                    {
                        _restrictions[restrictionPointer + 1 + i] = vertex1;
                    }
                    else if (_restrictions[restrictionPointer + 1 + i] == vertex1)
                    {
                        _restrictions[restrictionPointer + 1 + i] = vertex2;
                    }
                }

                this.AddByPointer(restrictionPointer);
            }
        }

        /// <summary>
        /// Adds a restriction using just it's pointer.
        /// </summary>
        private void AddByPointer(long pointer)
        {
            var size = _restrictions[pointer];

            for(var i = 0; i < size; i++)
            {
                this.AddPointer(_restrictions[pointer + 1 + i], pointer);
            }
        }

        /// <summary>
        /// Adds a pointer for the given vertex.
        /// </summary>
        private void AddPointer(long vertex, long pointer)
        {
            var hash = CalculateHash(vertex);
            var hashPointer = _hashes[hash];
            if (hashPointer == NO_DATA)
            { // add at the end.
                _hashes[hash] = _nextIndexPointer;
                _index.EnsureMinimumSize(_nextIndexPointer + 2);
                _index[_nextIndexPointer] = 1;
                _index[_nextIndexPointer + 1] = pointer;
                _nextIndexPointer += 2;
            }
            else
            { // add to the existing structure.
                var size = _index[hashPointer];

                // check if pointer is not already there.
                for (var i = 0; i < size; i++)
                {
                    if (_index[hashPointer + i + 1] == pointer)
                    { // pointer is already there.
                        return;
                    }
                }

                // add the pointer.
                if ((size & (size - 1)) == 0)
                { // a power of two, copy to the end.
                    var newSpace = size * 2;
                    _index.EnsureMinimumSize(_nextIndexPointer + newSpace + 2);
                    _index[_nextIndexPointer] = size;
                    for(var i = 0; i < size; i++)
                    {
                        _index[_nextIndexPointer + 1 + i] = _index[hashPointer + 1 + i];
                    }
                    hashPointer = _nextIndexPointer;
                    _hashes[hash] = hashPointer;
                    _nextIndexPointer += newSpace + 1;
                }
                _index[hashPointer + 1 + size] = pointer;
                size++;
                _index[hashPointer] = size;
            }
        }
        
        /// <summary>
        /// Removes a restriction by pointer.
        /// </summary>
        private void RemoveByPointer(long pointer)
        {
            var size = _restrictions[pointer];
            //_restrictions[pointer] = NO_DATA;
            
            for(var i = 0; i < size; i++)
            {
                var v = _restrictions[pointer + i + 1];
                //_restrictions[pointer + i + 1] = NO_DATA;

                var hash = CalculateHash(v);
                var hashPointer = _hashes[hash];
                if (hashPointer != NO_DATA)
                { // there is data.
                    var pSize = _index[hashPointer];
                    var j = 0;
                    for(; j < pSize; j++)
                    {
                        if (_index[hashPointer + j + 1] == pointer)
                        {
                            break;
                        }
                    }
                    for (; j < pSize; j++)
                    {
                        if (hashPointer + j + 2 < _index.Length)
                        {
                            _index[hashPointer + j + 1] = _index[hashPointer + j + 2];
                        }
                    }
                    _index[hashPointer] = pSize - 1;
                    if (pSize == 1)
                    {
                        _hashes[hash] = NO_DATA;
                    }
                }
            }
        }
        
        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public RestrictionEnumerator GetEnumerator()
        {
            return new RestrictionEnumerator(this);
        }

        /// <summary>
        /// Returns true if this db has complex restrictions.
        /// </summary>
        public bool HasComplexRestrictions
        {
            get
            {
                return _hasComplexRestrictions;
            }
        }

        /// <summary>
        /// Calculates a hashcode with a fixed size.
        /// </summary>
        private int CalculateHash(long vertex)
        {
            var hash = (vertex.GetHashCode() % (_hashes.Length / 2)) * 2;
            if (hash > 0)
            {
                return (int)hash;
            }
            return (int)-hash;
        }

        /// <summary>
        /// An enumerator to access restrictions.
        /// </summary>
        public class RestrictionEnumerator
        {
            private readonly RestrictionsDb _db;

            /// <summary>
            /// Creates a new restriction enumerator.
            /// </summary>
            internal RestrictionEnumerator(RestrictionsDb db)
            {
                _db = db;
            }

            private long _vertex;
            private long _indexPointer;
            private long _indexPosition;
            private long _restrictionPointer;
            
            /// <summary>
            /// Moves to restrictions for the given vertex.
            /// </summary>
            public bool MoveTo(long vertex)
            {
                _vertex = vertex;
                var hash = _db.CalculateHash(vertex);
                _indexPointer = _db._hashes[hash];
                if (_indexPointer == NO_DATA)
                { // a quick negative answer is the important part here!
                    _restrictionPointer = NO_DATA;
                    _indexPosition = NO_DATA;
                    return false;
                }

                var pSize = _db._index[_indexPointer];
                for (_indexPosition = 0; _indexPosition < pSize; _indexPosition++)
                { // check if this restriction has the given vertex as it's first.
                    _restrictionPointer = _db._index[_indexPointer + 1 + _indexPosition];
                    var restrictionSize = _db._restrictions[_restrictionPointer];
                    for (var i = 0; i < restrictionSize; i++)
                    {
                        if (_db._restrictions[_restrictionPointer + i + 1] == _vertex)
                        {
                            _restrictionPointer = NO_DATA; // we need to move here again.
                            return true;
                        }
                    }
                }
                _restrictionPointer = NO_DATA;
                _indexPointer = NO_DATA;
                _indexPosition = NO_DATA;
                return false;
            }

            /// <summary>
            /// Move to the next restriction.
            /// </summary>
            public bool MoveNext()
            {
                if (_indexPointer == NO_DATA)
                {
                    throw new Exception("Cannot move to next restriction when there is not current vertex.");
                }

                if (_restrictionPointer == NO_DATA)
                {
                    _restrictionPointer = _db._index[_indexPointer + 1 + _indexPosition];
                    return true;
                }

                var pSize = _db._index[_indexPointer];
                _indexPosition++;
                for (; _indexPosition < pSize; _indexPosition++)
                { // check if this restriction has the given vertex as it's first.
                    _restrictionPointer = _db._index[_indexPointer + 1 + _indexPosition];
                    var restrictionSize = _db._restrictions[_restrictionPointer];
                    for (var i = 0; i < restrictionSize; i++)
                    {
                        if (_db._restrictions[_restrictionPointer + i + 1] == _vertex)
                        {
                            return true;
                        }
                    }
                }
                _restrictionPointer = NO_DATA;
                _indexPointer = NO_DATA;
                _indexPosition = NO_DATA;
                return false;
            }

            /// <summary>
            /// Gets the number of vertices in the current restriction.
            /// </summary>
            public long Count
            {
                get
                {
                    if (_restrictionPointer == NO_DATA)
                    {
                        throw new InvalidOperationException("No current data available.");
                    }
                    
                    return _db._restrictions[_restrictionPointer];
                }
            }

            /// <summary>
            /// Returns the vertex at the given position.
            /// </summary>
            public long this[int i]
            {
                get
                {
                    if (_restrictionPointer == NO_DATA)
                    {
                        throw new InvalidOperationException("No current data available.");
                    }
                    return _db._restrictions[_restrictionPointer + 1 + i];
                }
            }

            /// <summary>
            /// Gets the id of this restriction.
            /// </summary>
            public long Id
            {
                get
                {
                    return _restrictionPointer;
                }
            }
        }

        private class LinkedNode
        {
            public long Pointer { get; set; }
            public LinkedNode Next { get; set; }
        }

        /// <summary>
        /// Serializes this restictions db to the given stream.
        /// </summary>
        public long Serialize(Stream stream)
        {
            long size = 1;
            // write the version #
            // 1: initial version.
            stream.WriteByte(1);

            // write hascomplexrestrictions.
            stream.WriteByte(_hasComplexRestrictions ? (byte)1 : (byte)0);
            size++;

            // write sizes, all longs, [count, hashCount, indexSize, restrictionSize]
            var bytes = BitConverter.GetBytes((long)_count);
            stream.Write(bytes, 0, 4);
            size += 4;
            bytes = BitConverter.GetBytes((long)_hashes.Length);
            stream.Write(bytes, 0, 4);
            size += 4;
            bytes = BitConverter.GetBytes(_nextIndexPointer);
            stream.Write(bytes, 0, 4);
            size += 4;
            bytes = BitConverter.GetBytes(_nextRestrictionPointer);
            stream.Write(bytes, 0, 4);
            size += 4;

            // write actual data, same order [hashes, index, restrictions]
            size += _hashes.CopyTo(stream);
            if (_index.Length > _nextIndexPointer)
            {
                _index.Resize(_nextIndexPointer);
            }
            size += _index.CopyTo(stream);
            if (_restrictions.Length > _nextRestrictionPointer)
            {
                _restrictions.Resize(_nextRestrictionPointer);
            }
            size += _restrictions.CopyTo(stream);

            return size;
        }

        /// <summary>
        /// Deserializes a restrictions db starting at the current position in the stream.
        /// </summary>
        public static RestrictionsDb Deserialize(Stream stream, RestrictionsDbProfile profile)
        {
            var version = stream.ReadByte();
            if (version > 1)
            {
                throw new Exception(string.Format("Cannot deserialize restrictions db: Invalid version #: {0}, upgrade Itinero.", version));
            }

            // read hascomplexrestrictions.
            var hasComplexRestrictionsByte = stream.ReadByte();
            var hasComplexRestrictions = hasComplexRestrictionsByte == 1;

            // read sizes.
            var bytes = new byte[32];
            stream.Read(bytes, 0, 32);
            var count = BitConverter.ToInt64(bytes, 0);
            var hashSize = BitConverter.ToInt64(bytes, 8);
            var indexSize = BitConverter.ToInt64(bytes, 16);
            var restrictionSize = BitConverter.ToInt64(bytes, 24);

            ArrayBase<long> hashes;
            if (profile == null || profile.HashesProfile == null)
            {
                hashes = Context.ArrayFactory.CreateMemoryBackedArray<long>(hashSize);
                hashes.CopyFrom(stream);
            }
            else
            {
                var position = stream.Position;
                var map = new MemoryMapStream(new CappedStream(stream, position,
                    hashSize * 4));
                hashes = new Array<long>(map.CreateInt64(hashSize), profile.HashesProfile);
                stream.Seek(hashSize * 8, SeekOrigin.Current);
            }

            ArrayBase<long> index;
            if (profile == null || profile.IndexProfile == null)
            {
                index = Context.ArrayFactory.CreateMemoryBackedArray<long>(indexSize);
                index.CopyFrom(stream);
            }
            else
            {
                var position = stream.Position;
                var map = new MemoryMapStream(new CappedStream(stream, position,
                    indexSize * 8));
                index = new Array<long>(map.CreateInt64(indexSize), profile.IndexProfile);
                stream.Seek(indexSize * 8, SeekOrigin.Current);
            }

            ArrayBase<long> restrictions;
            if (profile == null || profile.RestrictionsProfile == null)
            {
                restrictions = Context.ArrayFactory.CreateMemoryBackedArray<long>(restrictionSize);
                restrictions.CopyFrom(stream);
            }
            else
            {
                var position = stream.Position;
                var map = new MemoryMapStream(new CappedStream(stream, position,
                    restrictionSize * 8));
                restrictions = new Array<long>(map.CreateInt64(restrictionSize), profile.RestrictionsProfile);
                stream.Seek(restrictionSize * 8, SeekOrigin.Current);
            }

            return new RestrictionsDb(count, hasComplexRestrictions, hashes, index, restrictions);
        }
    }
}