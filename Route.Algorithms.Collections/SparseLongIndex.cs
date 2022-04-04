using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Route.Algorithms.Collections
{
    /// <summary>
    ///     An efficient index for a large number of bitflags that can handle both negative and positive ids.
    /// </summary>
    public class SparseLongIndex : IEnumerable<long>
    {
        private readonly int _blockSize = 64; // Holds the block size.
        private readonly long _size = 1024 * 1024 * (long)(1024 * 64); // Holds the total size.

        private SparseBitArray64 _negativeFlags; // Holds the negative flags array
        private SparseBitArray64 _positiveFlags; // Holds the positive flags array.

        /// <summary>
        ///     Creates a new longindex.
        /// </summary>
        public SparseLongIndex(int blockSize = 64)
        {
            _blockSize = blockSize;
        }

        /// <summary>
        ///     Returns the number of positive flags.
        /// </summary>
        public long Count { get; private set; }

        /// <summary>
        ///     Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<long> GetEnumerator()
        {
            if (_positiveFlags != null && _negativeFlags == null) return _positiveFlags.GetEnumerator();
            if (_positiveFlags == null && _negativeFlags != null) return _negativeFlags.GetEnumerator();
            return _negativeFlags.Concat(_positiveFlags).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }


        /// <summary>
        ///     Sets an id.
        /// </summary>
        public void Add(long number)
        {
            if (number >= 0)
                PositiveAdd(number);
            else
                NegativeAdd(-number);
        }

        /// <summary>
        ///     Removes an id.
        /// </summary>
        public void Remove(long number)
        {
            if (number >= 0)
                PositiveRemove(number);
            else
                NegativeAdd(-number);
        }

        /// <summary>
        ///     Returns true if the id is there.
        /// </summary>
        public bool Contains(long number)
        {
            if (number >= 0)
                return PositiveContains(number);
            return NegativeContains(-number);
        }

        /// <summary>
        ///     Clears this index.
        /// </summary>
        public void Clear()
        {
            _negativeFlags = null;
            _positiveFlags = null;
        }

        #region Positive

        /// <summary>
        ///     Adds an id.
        /// </summary>
        private void PositiveAdd(long number)
        {
            if (_positiveFlags == null) _positiveFlags = new SparseBitArray64(_size, _blockSize);

            if (!_positiveFlags[number])
                // there is a new positive flag.
                Count++;
            _positiveFlags[number] = true;
        }

        /// <summary>
        ///     Removes an id.
        /// </summary>
        private void PositiveRemove(long number)
        {
            if (_positiveFlags == null) _positiveFlags = new SparseBitArray64(_size, _blockSize);

            if (_positiveFlags[number])
                // there is one less positive flag.
                Count--;
            _positiveFlags[number] = false;
        }

        /// <summary>
        ///     Returns true if the id is there.
        /// </summary>
        private bool PositiveContains(long number)
        {
            if (_positiveFlags == null) return false;

            return _positiveFlags[number];
        }

        #endregion

        #region Negative

        /// <summary>
        ///     Adds an id.
        /// </summary>
        private void NegativeAdd(long number)
        {
            if (_negativeFlags == null) _negativeFlags = new SparseBitArray64(_size, _blockSize);

            if (!_negativeFlags[number])
                // there is one more negative flag.
                Count++;
            _negativeFlags[number] = true;
        }

        /// <summary>
        ///     Removes an id.
        /// </summary>
        private void NegativeRemove(long number)
        {
            if (_negativeFlags == null) _negativeFlags = new SparseBitArray64(_size, _blockSize);

            if (_negativeFlags[number])
                // there is one less negative flag.
                Count--;
            _negativeFlags[number] = false;
        }

        /// <summary>
        ///     Returns true if the id is there.
        /// </summary>
        private bool NegativeContains(long number)
        {
            if (_negativeFlags == null) return false;

            return _negativeFlags[number];
        }

        #endregion
    }
}