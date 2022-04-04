using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Reminiscence.Arrays;
using Reminiscence.Indexes;
using Reminiscence.IO;
using Reminiscence.IO.Streams;

namespace Route.Attributes
{
    /// <summary>
    ///     An index for attribute collections.
    /// </summary>
    public class AttributesIndex
    {
        private const long NULL_ATTRIBUTES = 0;
        private const long EMPTY_ATTRIBUTES = 1;
        private readonly Index<int[]> _collectionIndex;
        private readonly ArrayBase<long> _index;
        private readonly Index<string> _stringIndex;
        private IDictionary<int[], long> _collectionReverseIndex; // Holds all tag collections and their reverse index.

        private long _nextId;

        private IDictionary<string, int> _stringReverseIndex; // Holds all strings and their id.

        /// <summary>
        ///     Creates a new empty index.
        /// </summary>
        public AttributesIndex(AttributesIndexMode mode = AttributesIndexMode.ReverseCollectionIndex |
                                                          AttributesIndexMode.ReverseStringIndex)
        {
            _stringIndex = new Index<string>();
            _collectionIndex = new Index<int[]>();
            IsReadonly = false;
            IndexMode = mode;
            _stringReverseIndex = null;
            _collectionReverseIndex = null;

            if ((IndexMode & AttributesIndexMode.IncreaseOne) == AttributesIndexMode.IncreaseOne)
            {
                _index = Context.ArrayFactory.CreateMemoryBackedArray<long>(1024);
                _nextId = 0;
            }

            if ((IndexMode & AttributesIndexMode.ReverseStringIndex) == AttributesIndexMode.ReverseStringIndex ||
                (IndexMode & AttributesIndexMode.ReverseStringIndexKeysOnly) ==
                AttributesIndexMode.ReverseStringIndexKeysOnly)
                _stringReverseIndex = new Dictionary<string, int>();
            if ((IndexMode & AttributesIndexMode.ReverseCollectionIndex) == AttributesIndexMode.ReverseCollectionIndex)
                _collectionReverseIndex = new Dictionary<int[], long>(
                    new DelegateEqualityComparer<int[]>(
                        obj =>
                        {
                            // assumed the array is sorted.
                            var hash = obj.Length.GetHashCode();
                            for (var idx = 0; idx < obj.Length; idx++) hash = hash ^ obj[idx].GetHashCode();
                            return hash;
                        },
                        (x, y) =>
                        {
                            if (x.Length == y.Length)
                            {
                                for (var idx = 0; idx < x.Length; idx++)
                                    if (x[idx] != y[idx])
                                        return false;
                                return true;
                            }

                            return false;
                        }));
        }

        /// <summary>
        ///     Creates a new index.
        /// </summary>
        public AttributesIndex(MemoryMap map,
            AttributesIndexMode mode = AttributesIndexMode.ReverseCollectionIndex |
                                       AttributesIndexMode.ReverseStringIndex)
        {
            if (mode == AttributesIndexMode.None)
                throw new ArgumentException("Cannot create a new index without a valid operating mode.");

            _stringIndex = new Index<string>(map);
            _collectionIndex = new Index<int[]>(map);
            IsReadonly = false;
            IndexMode = mode;
            _stringReverseIndex = null;
            _collectionReverseIndex = null;

            if ((IndexMode & AttributesIndexMode.IncreaseOne) == AttributesIndexMode.IncreaseOne)
            {
                // create the increment-by-one data structures.
                _index = new Array<long>(map, 1024);
                _nextId = 0;
            }

            if ((IndexMode & AttributesIndexMode.ReverseStringIndex) == AttributesIndexMode.ReverseStringIndex ||
                (IndexMode & AttributesIndexMode.ReverseStringIndexKeysOnly) ==
                AttributesIndexMode.ReverseStringIndexKeysOnly)
                _stringReverseIndex = new Reminiscence.Collections.Dictionary<string, int>(map, 1024 * 16);
            if ((IndexMode & AttributesIndexMode.ReverseCollectionIndex) == AttributesIndexMode.ReverseCollectionIndex)
                _collectionReverseIndex = new Reminiscence.Collections.Dictionary<int[], long>(map, 1024 * 16,
                    new DelegateEqualityComparer<int[]>(
                        obj =>
                        {
                            // assumed the array is sorted.
                            var hash = obj.Length.GetHashCode();
                            for (var idx = 0; idx < obj.Length; idx++) hash = hash ^ obj[idx].GetHashCode();
                            return hash;
                        },
                        (x, y) =>
                        {
                            if (x.Length == y.Length)
                            {
                                for (var idx = 0; idx < x.Length; idx++)
                                    if (x[idx] != y[idx])
                                        return false;
                                return true;
                            }

                            return false;
                        }));
        }

        /// <summary>
        ///     Creates a new index.
        /// </summary>
        internal AttributesIndex(AttributesIndexMode mode, Index<string> stringIndex, Index<int[]> tagsIndex)
        {
            _stringIndex = stringIndex;
            _collectionIndex = tagsIndex;
            IsReadonly = true;
            _index = null;
            _nextId = long.MaxValue;
            IndexMode = mode;

            _stringReverseIndex = null;
            _collectionReverseIndex = null;
        }

        /// <summary>
        ///     Creates a new index.
        /// </summary>
        internal AttributesIndex(AttributesIndexMode mode, Index<string> stringIndex, Index<int[]> tagsIndex,
            ArrayBase<long> index)
        {
            _stringIndex = stringIndex;
            _collectionIndex = tagsIndex;
            IsReadonly = true;
            _index = index;
            _nextId = index.Length;
            IndexMode = mode;

            _stringReverseIndex = null;
            _collectionReverseIndex = null;
        }

        /// <summary>
        ///     Returns true if this index is readonly.
        /// </summary>
        public bool IsReadonly { get; private set; }

        /// <summary>
        ///     Returns true if this index checks for duplicates.
        /// </summary>
        public bool CheckDuplicates => _stringReverseIndex != null;

        /// <summary>
        ///     Gets the number of collections.
        /// </summary>
        public long Count
        {
            get
            {
                if ((IndexMode & AttributesIndexMode.IncreaseOne) == AttributesIndexMode.IncreaseOne)
                    // uses increase one.
                    return _nextId + 2;
                if ((IndexMode & AttributesIndexMode.None) == AttributesIndexMode.None && _index != null)
                    // deserialized but used increase one before.
                    return _nextId + 2;
                throw new Exception("Count cannot be calculated on a index that doesn't use 'IncreaseOne' mode.");
            }
        }

        /// <summary>
        ///     Returns the index mode.
        /// </summary>
        public AttributesIndexMode IndexMode { get; }

        /// <summary>
        ///     Returns the attributes that belong to the given id.
        /// </summary>
        public IAttributeCollection Get(long tagsId)
        {
            if (tagsId == 0)
                return null;
            if (tagsId == 1) return new AttributeCollection();
            if (_index != null)
            {
                // use the index if it's there.
                tagsId = _index[tagsId - 2];
                return new InternalAttributeCollection(_stringIndex, _collectionIndex.Get(tagsId));
            }

            return new InternalAttributeCollection(_stringIndex, _collectionIndex.Get(tagsId - 2));
        }

        /// <summary>
        ///     Adds new attributes.
        /// </summary>
        public long Add(IAttributeCollection tags)
        {
            if (tags == null)
                return NULL_ATTRIBUTES;
            if (tags.Count == 0) return EMPTY_ATTRIBUTES;

            if (IsReadonly)
            {
                // this index is readonly.
                // TODO: make writeable.
                // - set nextId.
                // - create reverse indexes if needed.
                if (_index != null)
                {
                    // this should be an increase-one index.
                    if ((IndexMode & AttributesIndexMode.IncreaseOne) != AttributesIndexMode.IncreaseOne)
                        throw new Exception(
                            "Invalid combination of data: There is an index but mode isn't increase one.");

                    _nextId = _index.Length;
                }

                // build reverse indexes if needed.
                if ((IndexMode & AttributesIndexMode.ReverseStringIndex) == AttributesIndexMode.ReverseStringIndex ||
                    (IndexMode & AttributesIndexMode.ReverseStringIndexKeysOnly) ==
                    AttributesIndexMode.ReverseStringIndexKeysOnly)
                {
                    _stringReverseIndex = new Reminiscence.Collections.Dictionary<string, int>(
                        new MemoryMapStream(), 1024 * 16);

                    // add existing data.
                    if ((IndexMode & AttributesIndexMode.ReverseStringIndex) == AttributesIndexMode.ReverseStringIndex)
                        // build reverse index for all data.
                        foreach (var pair in _stringIndex)
                            _stringReverseIndex[pair.Value] = (int)pair.Key;
                    else
                        // build reverse index for keys only.
                        foreach (var collectionPair in _collectionIndex)
                        foreach (var stringId in collectionPair.Value)
                            _stringReverseIndex[_stringIndex.Get(stringId)] = stringId;
                }

                if ((IndexMode & AttributesIndexMode.ReverseCollectionIndex) ==
                    AttributesIndexMode.ReverseCollectionIndex)
                {
                    _collectionReverseIndex = new Reminiscence.Collections.Dictionary<int[], long>(
                        new MemoryMapStream(),
                        1024 * 16,
                        new DelegateEqualityComparer<int[]>(
                            obj =>
                            {
                                // assumed the array is sorted.
                                var hash = obj.Length.GetHashCode();
                                for (var idx = 0; idx < obj.Length; idx++) hash = hash ^ obj[idx].GetHashCode();
                                return hash;
                            },
                            (x, y) =>
                            {
                                if (x.Length == y.Length)
                                {
                                    for (var idx = 0; idx < x.Length; idx++)
                                        if (x[idx] != y[idx])
                                            return false;
                                    return true;
                                }

                                return false;
                            }));
                    if (_index != null)
                        for (long col = 0; col < _nextId; col++)
                        {
                            var pointer = _index[col];
                            _collectionReverseIndex[_collectionIndex.Get(pointer)] = col;
                        }
                    else
                        foreach (var pair in _collectionIndex)
                            _collectionReverseIndex[pair.Value] = pair.Key;
                }

                IsReadonly = false;
            }

            // add new collection.
            var sortedSet = new Algorithms.Collections.SortedSet<long>();
            foreach (var tag in tags)
                sortedSet.Add(AddString(tag.Key, true) +
                              int.MaxValue * (long)AddString(tag.Value, false));

            // sort keys.
            var sorted = new int[sortedSet.Count * 2];
            var i = 0;
            foreach (var pair in sortedSet)
            {
                sorted[i] = (int)(pair % int.MaxValue);
                i++;
                sorted[i] = (int)(pair / int.MaxValue);
                i++;
            }

            // add sorted collection.
            return AddCollection(sorted);
        }

        /// <summary>
        ///     Adds a new string.
        /// </summary>
        private int AddString(string value, bool key)
        {
            int id;
            if ((IndexMode & AttributesIndexMode.ReverseStringIndex) == AttributesIndexMode.ReverseStringIndex ||
                (IndexMode & AttributesIndexMode.ReverseStringIndexKeysOnly) ==
                AttributesIndexMode.ReverseStringIndexKeysOnly && key)
            {
                if (!_stringReverseIndex.TryGetValue(value, out id))
                {
                    // the key doesn't exist yet.
                    id = (int)_stringIndex.Add(value);
                    _stringReverseIndex.Add(value, id);
                }

                return id;
            }

            return (int)_stringIndex.Add(value);
        }

        /// <summary>
        ///     Adds a new collection, it's assumed to be sorted.
        /// </summary>
        private long AddCollection(int[] collection)
        {
            long id;
            if (_collectionReverseIndex != null)
                // check duplicates.
                if (_collectionReverseIndex.TryGetValue(collection, out id))
                    // collection already exists.
                    return id + 2;

            id = _collectionIndex.Add(collection);
            if (_index != null)
            {
                // use next id.
                _index.EnsureMinimumSize(_nextId + 1);
                _index[_nextId] = id;
                id = _nextId;
                _nextId++;
            }

            if (_collectionReverseIndex != null) _collectionReverseIndex.Add(collection, id);
            return id + 2;
        }

        /// <summary>
        ///     An implementation of a tags collection.
        /// </summary>
        private class InternalAttributeCollection : IAttributeCollection
        {
            private readonly Index<string> _stringIndex; // Holds the string index.
            private readonly int[] _tags; // Holds the tags.

            /// <summary>
            ///     Creates a new internal attributes collection.
            /// </summary>
            public InternalAttributeCollection(Index<string> stringIndex, int[] tags)
            {
                _stringIndex = stringIndex;
                _tags = tags;
            }

            /// <summary>
            ///     Returns the number of attributes in this collection.
            /// </summary>
            public int Count => _tags.Length / 2;

            /// <summary>
            ///     Returns true if this collection is readonly.
            /// </summary>
            public bool IsReadonly => true;

            /// <summary>
            ///     Returns true if the given tag exists.
            /// </summary>
            public bool TryGetValue(string key, out string value)
            {
                for (var i = 0; i < _tags.Length; i = i + 2)
                    if (key == _stringIndex.Get(_tags[i]))
                    {
                        value = _stringIndex.Get(_tags[i + 1]);
                        return true;
                    }

                value = null;
                return false;
            }

            /// <summary>
            ///     Removes the attribute with the given key.
            /// </summary>
            public bool RemoveKey(string key)
            {
                throw new InvalidOperationException("This attribute collection is readonly. Check IsReadonly.");
            }

            /// <summary>
            ///     Adds or replaces an attribute.
            /// </summary>
            public void AddOrReplace(string key, string value)
            {
                throw new InvalidOperationException("This attribute collection is readonly. Check IsReadonly.");
            }

            /// <summary>
            ///     Clears all attributes.
            /// </summary>
            public void Clear()
            {
                throw new InvalidOperationException("This attribute collection is readonly. Check IsReadonly.");
            }

            /// <summary>
            ///     Returns the enumerator for this enumerable.
            /// </summary>
            public IEnumerator<Attribute> GetEnumerator()
            {
                return new InternalTagsEnumerator(_stringIndex, _tags);
            }

            /// <summary>
            ///     Returns the enumerator for this enumerable.
            /// </summary>
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            /// <summary>
            ///     Gets a proper description of this attribute collection.
            /// </summary>
            /// <returns></returns>
            public override string ToString()
            {
                var builder = new StringBuilder();
                foreach (var a in this)
                {
                    if (builder.Length > 0) builder.Append('|');
                    builder.Append(a.ToString());
                }

                return builder.ToString();
            }
        }

        /// <summary>
        ///     An internal implementation of an attribute enumerator.
        /// </summary>
        private class InternalTagsEnumerator : IEnumerator<Attribute>
        {
            /// <summary>
            ///     Holds the current idx.
            /// </summary>
            private int _idx = -2;

            private Index<string> _stringIndex; // Holds the string index.
            private int[] _tags; // Holds the tags.

            /// <summary>
            ///     Creates a new internal tags collection.
            /// </summary>
            public InternalTagsEnumerator(Index<string> stringIndex, int[] tags)
            {
                _stringIndex = stringIndex;
                _tags = tags;
            }

            /// <summary>
            ///     Returns the current tag.
            /// </summary>

            public Attribute Current =>
                new Attribute
                {
                    Key = _stringIndex.Get(_tags[_idx]),
                    Value = _stringIndex.Get(_tags[_idx + 1])
                };

            /// <summary>
            ///     Returns the current tag.
            /// </summary>
            object IEnumerator.Current =>
                new Attribute
                {
                    Key = _stringIndex.Get(_tags[_idx]),
                    Value = _stringIndex.Get(_tags[_idx + 1])
                };

            /// <summary>
            ///     Move to the next attribute.
            /// </summary>
            public bool MoveNext()
            {
                _idx = _idx + 2;
                return _idx < _tags.Length;
            }

            /// <summary>
            ///     Resets this enumerator.
            /// </summary>
            public void Reset()
            {
                _idx = -2;
            }

            /// <summary>
            ///     Disposes this enumerator.
            /// </summary>
            public void Dispose()
            {
                _tags = null;
                _stringIndex = null;
            }
        }

        #region Serialization

        /// <summary>
        ///     Serializes this tags index to the given stream.
        /// </summary>
        public long Serialize(Stream stream)
        {
            // version history.
            // version 0-1: unused, fallback to unversioned.
            // version 2: first version that contains information to make indexed writable again.

            // write version #.
            long size = 1;
            stream.WriteByte(2);

            // write index type flags.
            size++;
            stream.WriteByte((byte)IndexMode);

            // write the actual data.
            if (_index == null)
            {
                // this is a regular index.
                stream.WriteByte(0);
                size++;
                size += _collectionIndex.CopyToWithSize(stream);
                size += _stringIndex.CopyToWithSize(stream);
            }
            else
            {
                // this is an increase one index.
                // compress index.
                _index.Resize(_nextId);

                stream.WriteByte(1);
                size++;
                size += _collectionIndex.CopyToWithSize(stream);
                size += _stringIndex.CopyToWithSize(stream);
                stream.Write(BitConverter.GetBytes(_index.Length), 0, 8);
                size += 8;
                size += _index.CopyTo(stream);
            }

            return size;
        }

        /// <summary>
        ///     Deserializes a tags index from the given stream.
        /// </summary>
        public static AttributesIndex Deserialize(Stream stream, bool copy = false,
            AttributesIndexMode defaultIndexMode = AttributesIndexMode.ReverseStringIndexKeysOnly)
        {
            // read version byte.
            long position = 1;
            var version = stream.ReadByte();

            var type = 0;
            if (version < 2)
            {
                // unversioned version.
                type = (byte)version;
            }
            else
            {
                // versioned.
                // read the index mode.
                var indexModeByte = stream.ReadByte();
                position++;
                defaultIndexMode = (AttributesIndexMode)indexModeByte;

                // read the type.
                type = stream.ReadByte();
                position++;
            }

            // read the actual data.
            long size;
            if (type == 0)
            {
                // regular index.
                var tagsIndex = Index<int[]>.CreateFromWithSize(stream, out size, !copy);
                position += size + 8;
                stream.Seek(position, SeekOrigin.Begin);
                var limitedStream = new LimitedStream(stream);
                var stringIndex = Index<string>.CreateFromWithSize(limitedStream, out size, !copy);
                position += size + 8;
                stream.Seek(position, SeekOrigin.Begin);
                return new AttributesIndex(defaultIndexMode, stringIndex, tagsIndex);
            }
            else
            {
                // increase one index.
                var tagsIndex = Index<int[]>.CreateFromWithSize(stream, out size, !copy);
                position += size + 8;
                stream.Seek(position, SeekOrigin.Begin);
                var limitedStream = new LimitedStream(stream);
                var stringIndex = Index<string>.CreateFromWithSize(limitedStream, out size, !copy);
                position += size + 8;
                stream.Seek(position, SeekOrigin.Begin);
                var indexLengthBytes = new byte[8];
                stream.Read(indexLengthBytes, 0, 8);
                var indexLength = BitConverter.ToInt64(indexLengthBytes, 0);
                var index = Context.ArrayFactory.CreateMemoryBackedArray<long>(indexLength);
                index.CopyFrom(stream);
                return new AttributesIndex(defaultIndexMode, stringIndex, tagsIndex, index);
            }
        }

        #endregion
    }

    /// <summary>
    ///     Attributes index mode flags.
    /// </summary>
    [Flags]
    public enum AttributesIndexMode
    {
        /// <summary>
        ///     No specific mode, mode is about writing, used only when readonly.
        /// </summary>
        None = 0x0,

        /// <summary>
        ///     Increase id's by one.
        /// </summary>
        IncreaseOne = 0x1,

        /// <summary>
        ///     Keep a reverse collection index.
        /// </summary>
        ReverseCollectionIndex = 0x2,

        /// <summary>
        ///     Keep a reverse string index.
        /// </summary>
        ReverseStringIndex = 0x4,

        /// <summary>
        ///     Only keep a reverse index of keys.
        /// </summary>
        ReverseStringIndexKeysOnly = 0x8,

        /// <summary>
        ///     All reverse indexes active.
        /// </summary>
        ReverseAll = 0x2 + 0x4
    }
}