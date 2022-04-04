using System;

namespace Route.Algorithms.Collections
{
    /// <summary>
    ///     Represents a large bit array.
    /// </summary>
    public class BitArray64
    {
        private readonly long[] _array; // Holds the bit vector array.

        /// <summary>
        ///     Creates a new bitvector array.
        /// </summary>
        public BitArray64(long size)
        {
            Length = size;
            _array = new long[(int)Math.Ceiling((double)size / 64)];
        }

        /// <summary>
        ///     Returns the element at the given index.
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
                    // set value.
                    _array[arrayIdx] = mask | _array[arrayIdx];
                else
                    // unset value.
                    _array[arrayIdx] = ~mask & _array[arrayIdx];
            }
        }

        /// <summary>
        ///     Returns the length of this array.
        /// </summary>
        public long Length { get; }
    }
}