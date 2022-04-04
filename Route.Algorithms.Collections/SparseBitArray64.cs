using System;
using System.Collections;
using System.Collections.Generic;

namespace Route.Algorithms.Collections
{
    /// <summary>
    ///     Represents a sparse bitarray.
    /// </summary>
    public class SparseBitArray64 : IEnumerable<long>
    {
        private readonly int _blockSize; // Holds the blocksize, or the size of the 'sub arrays'.
        private readonly HugeDictionary<long, BitArray> _data; // holds the actual data blocks.

        //private readonly BitArray32[] _data; // Holds the actual data blocks.

        /// <summary>
        ///     Creates a new sparse bitarray.
        /// </summary>
        public SparseBitArray64(long size, int blockSize)
        {
            if (size % 64 != 0) throw new ArgumentOutOfRangeException("Size has to be divisible by 64.");
            if (size % blockSize != 0) throw new ArgumentOutOfRangeException("Size has to be divisible by blocksize.");

            Length = size;
            _blockSize = blockSize;
            _data = new HugeDictionary<long, BitArray>(); // BitArray32[_length / _blockSize];
        }

        /// <summary>
        ///     Gets or sets the value at the given index.
        /// </summary>
        public bool this[long idx]
        {
            get
            {
                var blockId = (int)(idx / _blockSize);
                BitArray block = null;
                if (_data.TryGetValue(blockId, out block))
                {
                    // the block actually exists.
                    var blockIdx = (int)(idx % _blockSize);
                    return _data[blockId][blockIdx];
                }

                return false;
            }
            set
            {
                var blockId = (int)(idx / _blockSize);
                BitArray block = null;
                if (!_data.TryGetValue(blockId, out block))
                {
                    if (value)
                    {
                        // only add new block if true.
                        block = new BitArray(_blockSize);
                        var blockIdx = (int)(idx % _blockSize);
                        block[blockIdx] = true;
                        _data[blockId] = block;
                    }
                }
                else
                {
                    // set value at block.
                    var blockIdx = (int)(idx % _blockSize);
                    block[blockIdx] = value;
                }
            }
        }

        /// <summary>
        ///     Returns the length of this array.
        /// </summary>
        public long Length { get; }

        /// <summary>
        ///     Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<long> GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(this);
        }

        private struct Enumerator : IEnumerator<long>
        {
            private readonly SparseBitArray64 _array;

            public Enumerator(SparseBitArray64 array)
            {
                _array = array;
                Current = -1;
            }

            public long Current { get; private set; }

            object IEnumerator.Current
            {
                get
                {
                    if (Current < 0) throw new InvalidOperationException();
                    if (Current >= _array.Length) throw new InvalidOperationException();
                    return Current;
                }
            }

            public bool MoveNext()
            {
                if (Current >= _array.Length) return false;

                while (true)
                {
                    Current++;
                    if (Current >= _array.Length) return false;
                    if (_array[Current]) break;
                }

                return true;
            }

            public void Reset()
            {
                Current = -1;
            }

            public void Dispose()
            {
            }
        }
    }
}